/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.openmetadata.adapters.eventmapper;



import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.notification.entity.EntityMessageDeserializer;
import org.apache.atlas.openmetadata.adapters.repositoryconnector.LocalAtlasOMRSErrorCode;
import org.apache.atlas.openmetadata.adapters.repositoryconnector.LocalAtlasOMRSMetadataCollection;
import org.apache.atlas.openmetadata.adapters.repositoryconnector.LocalAtlasOMRSRepositoryConnector;
import static org.apache.atlas.kafka.KafkaNotification.ATLAS_ENTITIES_TOPIC;

import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.repositoryservices.auditlog.OMRSAuditLog;
import org.odpi.openmetadata.repositoryservices.auditlog.OMRSAuditingComponent;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.EntityDetail;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceType;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryeventmapper.OMRSRepositoryEventMapperBase;
import org.odpi.openmetadata.repositoryservices.events.OMRSEventCategory;
import org.odpi.openmetadata.repositoryservices.events.OMRSEventOriginator;
import org.odpi.openmetadata.repositoryservices.events.OMRSInstanceEventType;
import org.odpi.openmetadata.repositoryservices.events.beans.v1.OMRSEventV1;
import org.odpi.openmetadata.repositoryservices.events.beans.v1.OMRSEventV1InstanceSection;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;




/**
 * AtlasOMRSRepositoryEventMapper provides an implementation of a repository event mapper for the
 * Apache Atlas metadata repository.
 */

public class AtlasOMRSRepositoryEventMapper extends OMRSRepositoryEventMapperBase {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasOMRSRepositoryEventMapper.class);
    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.LOCAL_REPOSITORY_EVENT_MAPPER);

    // Kafka Topic Consumer
    // Some properties can be hard coded but the bootstrap server need to be read from the Atlas properties
    private static final String TOPIC_NAME                 = ATLAS_ENTITIES_TOPIC;
    private static final String CONSUMER_GROUP_ID          = "AtlasOMRSRepositoryEventMapperConsumerGroup";  // OK to hard code - only the event mapper should be using this
    private static final String BOOTSTRAP_SERVERS_DEFAULT  = "localhost:9027";
    private static final String BOOTSTRAP_SERVERS_PROPERTY = "atlas.kafka.bootstrap.servers";

    private String                           bootstrapServers   = null;
    private RunnableConsumer                 runnableConsumer   = null;
    private Thread                           consumerThread     = null;
    private EntityMessageDeserializer        deserializer       = new EntityMessageDeserializer();
    private LocalAtlasOMRSMetadataCollection metadataCollection = null;

    /**
     * Default constructor
     */
    public AtlasOMRSRepositoryEventMapper() throws RepositoryErrorException {
        super();
        LOG.debug("AtlasOMRSRepositoryEventMapper constructor invoked");

        try {
            bootstrapServers = ApplicationProperties.get().getString(BOOTSTRAP_SERVERS_PROPERTY, BOOTSTRAP_SERVERS_DEFAULT);
            LOG.debug("AtlasOMRSRepositoryEventMapper: bootstrapServers {}",bootstrapServers);
        } catch (AtlasException e) {
            LOG.error("AtlasOMRSRepositoryEventMapper: Could not find bootstrap servers, giving up");
            String actionDescription = "LocalAtlasOMRSMetadataCollection Constructor";

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.ATLAS_CONFIGURATION;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage();

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    actionDescription,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        this.runnableConsumer = createConsumer();

    }

    private RunnableConsumer createConsumer() {

        KafkaConsumer consumer = null;

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);

        // Deserialization - at this level should support Kafka records - String,String should be sufficient
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        RunnableConsumer runnableConsumer = new RunnableConsumer(consumer);

        return runnableConsumer;

    }

    public class RunnableConsumer implements Runnable {
        private final KafkaConsumer consumer;
        private       boolean       keepOnRunning;

        // package-private
        RunnableConsumer(KafkaConsumer consumer) {
            this.consumer = consumer;
            keepOnRunning = true;
        }

        // package-private
        void stopRunning() {
            keepOnRunning = false;
        }

        @Override
        public void run() {

            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasOMRSRepositoryEventMapper RunnableConsumer.run()");
            }

            while (keepOnRunning) {

                ConsumerRecords<?, ?> records  = consumer.poll(1000);
                if (records != null) {

                     /* There is a very real risk that the transaction that generated the event has not yet committed. This
                      * is not a hypothetical situation - it happens. It is possible for the Kafka producer to generate the
                      * notification and for our consumer to have received it prior to the commit of the creating transaction.
                      * We therefore cannot assume that it will be possible to get the latest entity details, latest
                      * classifications, etc. from the entity store.
                      * The strategy adopted below is to yield. When we get control back, if the entity has not been updated
                      * (or created) then the event mapper will do what it can with the currently available state. We could
                      * introduce a more elaborate strategy in which the event (or entity header) is stashed and the stash is
                      * serviced periodically, e.g. just before each consumer poll. Alternatively we could implement a "mapper
                      * makes good" strategy in which the event mapper waits for the entity GUID to exist, or for the listed
                      * classifications to have appeared on the entity. However, there are some types of notification for which
                      * the event mapper cannot know the desired state of the entity/classifications, so it would be futile to
                      * try and guess. The strategy implemented here is simpler - it yields and then does it's best.
                      */

                    Long TRANSACTION_WAIT_TIME = 1000L;
                    try {
                        Thread.sleep(TRANSACTION_WAIT_TIME);
                    }
                    catch (InterruptedException interrupted) {
                        // That's OK, continue...
                        continue;
                    }

                    for (ConsumerRecord<?, ?> record : records) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received Message topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        }

                        EntityNotification message = deserializer.deserialize(record.value().toString());
                        if (message != null) {
                            processMessage(message);
                        }
                    }
                }
                consumer.commitAsync();
            }
            consumer.close();

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== RunnableConsumer.run() ending");
            }
        }
    }

    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException there is a problem within the connector.
     */
    public void start() throws ConnectorCheckedException  {

        final String methodName = "start";

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasOMRSRepositoryEventMapper start()");
        }
        super.start();

        LOG.debug("AtlasOMRSRepositoryEventMapper.start: metadataCollectionId {}, repositoryConnector {}", localMetadataCollectionId, repositoryConnector);

        // Get and verify the metadataCollection...
        boolean metadataCollectionOK = false;
        LocalAtlasOMRSRepositoryConnector repositoryConnector = (LocalAtlasOMRSRepositoryConnector) this.repositoryConnector;
        try {
            metadataCollection = (LocalAtlasOMRSMetadataCollection) repositoryConnector.getMetadataCollection();
        }
        catch (RepositoryErrorException e) {
            LOG.error("AtlasOMRSRepositoryEventMapper.start: Exception from getMetadataCollection, message = {}", e.getMessage());
            metadataCollectionOK = false;
        }
        if (metadataCollection != null) {
            // Check that the metadataCollection is responding...
            try {
                String id = metadataCollection.getMetadataCollectionId();
                if (id.equals(localMetadataCollectionId)) {
                    LOG.debug("AtlasOMRSRepositoryEventMapper.start: metadataCollection verified");
                    metadataCollectionOK = true;
                } else {
                    LOG.error("AtlasOMRSRepositoryEventMapper.start: Could not retrieve metadataCollection");
                    metadataCollectionOK = false;
                }
            } catch (RepositoryErrorException e) {
                metadataCollectionOK = false;
            }
        }
        if (!metadataCollectionOK) {
            LOG.error("AtlasOMRSRepositoryEventMapper.ctor: Could not access metadata collection");

            LocalAtlasOMRSErrorCode errorCode = LocalAtlasOMRSErrorCode.METADATA_COLLECTION_NOT_FOUND;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(localMetadataCollectionId);

            throw new ConnectorCheckedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());

        }

        if (this.runnableConsumer == null) {
            LOG.error("AtlasOMRSRepositoryEventMapper: No runnable consumer!!!");

           OMRSErrorCode errorCode = OMRSErrorCode.REPOSITORY_LOGIC_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(repositoryEventMapperName, methodName, "RunnableConsumer not created");

            throw new ConnectorCheckedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        else {
            consumerThread = new Thread(runnableConsumer);
            consumerThread.setDaemon(true);
            consumerThread.start();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasOMRSRepositoryEventMapper start()");
        }
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void disconnect() throws ConnectorCheckedException {
        LOG.debug("AtlasOMRSRepositoryEventMapper disconnect()");
        runnableConsumer.stopRunning();
        LOG.debug("AtlasOMRSRepositoryEventMapper: runnable consumer thread told to stop");
        super.disconnect();

    }

    /**
     * Method to process an EntityNotification message
     *
     * @param message - message received from ATLAS_ENTITIES topic
     */
    public void processMessage(EntityNotification message) {

        LOG.debug("AtlasOMRSRepositoryEventMapper.processMessage {}", message);

        // check the notification version (sometimes referred to as 'type')
        EntityNotification.EntityNotificationType version = message.getType();
        switch (version) {

            case ENTITY_NOTIFICATION_V2:
                EntityNotification.EntityNotificationV2 messageV2 = (EntityNotification.EntityNotificationV2)message;
                toOMRSEventV1(messageV2);
                break;

            case ENTITY_NOTIFICATION_V1:
            default:
                LOG.error("AtlasOMRSRepositoryEventMapper.processMessage: Message skipped!! - not expecting a V1 entity notification...");
                // skip the message...
                break;
        }

    }


    public void toOMRSEventV1(EntityNotification.EntityNotificationV2 notification) {

        final String methodName = "AtlasOMRSRepositoryEventMapper.toOMRSEventV1";

        if (LOG.isDebugEnabled()) {
            LOG.debug("{}: notification={}", methodName, notification);
        }
        OMRSEventV1 omrsEventV1 = null;

        AtlasEntityHeader atlasEntityHeader = notification.getEntity();
        LOG.debug("{}: atlasEntityHeader={}", methodName, atlasEntityHeader);


        String atlasEntityGuid = atlasEntityHeader.getGuid();
        String userId = "FABRICATED_USER"; // TODO!! need to figure out how to get real userId here...

        EntityDetail entityDetail = null;

        try {
            boolean isProxy = metadataCollection.isEntityProxy(atlasEntityGuid);
            if (isProxy) {
                // The entity is a proxy - it was added by an OMRS action and we are not interested in this atlas event
                LOG.debug("{}: event relates to an EntityProxy with guid {} - ignored", methodName, atlasEntityGuid);
                return;
            }
            else {
                // Check if it is a remote object and if so ignore it ...
                boolean isLocal = metadataCollection.isEntityLocal(atlasEntityGuid);
                if (!isLocal) {
                    // The entity is remote - we are not interested in this atlas event
                    LOG.debug("{}: event relates to a remotely mastered Entity with guid {} - ignored", methodName, atlasEntityGuid);
                    return;
                }
                else{ // (!isProxy && isLocal)
                    LOG.debug("{}: event relates to a locally mastered Entity with guid {} - process", methodName, atlasEntityGuid);
                    entityDetail = metadataCollection.getEntityDetail(userId, atlasEntityGuid);
                }
            }
        } catch (Exception e) {
            LOG.error("{}: Exception from metadataCollection, message={}", methodName, e.getMessage());
            /* It is possible that the event is plain wrong - but much more likely that the eent relates to a transaction that has not yet
             * committed. Since we already waiting for transaction to commit then unless we want to adopt an approach herre of looping and checking
             * not just entity existence but correct classification membership then we should fail here. There are some things that we could attempt
             * to "make good" here by waiting and retrying, but there are others (e.g. on an entity update event) where we cannot tell when the
             * transaction has actually completed. So adopt a siple approach of wait once - try once - fail.
             */

            // Ignore the event...
            return;
        }


        if (entityDetail != null) {

            omrsEventV1 = new OMRSEventV1();

            Date now = new Date();
            omrsEventV1.setTimestamp(now);

            OMRSEventOriginator originator = new OMRSEventOriginator();
            originator.setMetadataCollectionId(this.localMetadataCollectionId);
            omrsEventV1.setOriginator(originator);

            omrsEventV1.setEventCategory(OMRSEventCategory.INSTANCE);

            OMRSEventV1InstanceSection instanceSection = new OMRSEventV1InstanceSection();


            LOG.debug("{}: entity detail = {}", methodName, entityDetail);

            // Construct OMRSEventV1
            // If we do not have a valid InstanceType log an error and give up on the event
            InstanceType entityinstanceType = entityDetail.getType();
            if (entityinstanceType == null) {
                LOG.error("{}: EntityDetail does not have valid InstanceType, entityDetail = {}", methodName, entityDetail);
                return;
            }
            String typeDefGUID = entityinstanceType.getTypeDefGUID();
            instanceSection.setTypeDefGUID(typeDefGUID);

            String typeDefName = entityinstanceType.getTypeDefName();
            instanceSection.setTypeDefName(typeDefName);

            instanceSection.setInstanceGUID(atlasEntityGuid);

            instanceSection.setOriginalEntity(null);

            instanceSection.setEntity(entityDetail);

            instanceSection.setOriginalRelationship(null);
            instanceSection.setRelationship(null);
            instanceSection.setHomeMetadataCollectionId(localMetadataCollectionId);
            instanceSection.setOriginalHomeMetadataCollectionId(null);
            instanceSection.setOriginalHomeMetadataCollectionId(null);
            instanceSection.setOriginalTypeDefSummary(null);
            instanceSection.setOriginalInstanceGUID(null);

            omrsEventV1.setInstanceEventSection(instanceSection);

            // We have not yet set the eventType but that is done inline below with the call to the event processor...

            // Atlas does not yet generate undone, purged, restored, rehomed, reidentified or retyped events or refresh requests or events
            // It doesn#t support relationship events yet either
            // Nor is there any support for conflicting types or conflicting instances events

            EntityNotification.EntityNotificationV2.OperationType operationType = notification.getOperationType();
            LOG.debug("{}: Publish OMRSEventV1, operationType {}, typeDefGUID {}, typeDefName {}",
                    methodName, operationType, typeDefGUID, typeDefName );
            switch (operationType) {
                case ENTITY_CREATE:
                    instanceSection.setEventType(OMRSInstanceEventType.NEW_ENTITY_EVENT);
                    LOG.debug("{}: invoke processNewEntityEvent", methodName);
                    repositoryEventProcessor.processNewEntityEvent(
                            repositoryEventMapperName,
                            localMetadataCollectionId,
                            localServerName,
                            localServerType,
                            localOrganizationName,
                            entityDetail);
                    break;
                case ENTITY_UPDATE:
                    instanceSection.setEventType(OMRSInstanceEventType.UPDATED_ENTITY_EVENT);
                    LOG.debug("{}: invoke processUpdatedEntityEvent", methodName);
                    repositoryEventProcessor.processUpdatedEntityEvent(
                            repositoryEventMapperName,
                            localMetadataCollectionId,
                            localServerName,
                            localServerType,
                            localOrganizationName,
                            null,        // We do not have the old entity - this will be addressed further up the stack if available
                            entityDetail);
                    break;
                case ENTITY_DELETE:
                    instanceSection.setEventType(OMRSInstanceEventType.DELETED_ENTITY_EVENT);
                    LOG.debug("{}: invoke processDeletedEntityEvent", methodName);
                    repositoryEventProcessor.processDeletedEntityEvent(
                            repositoryEventMapperName,
                            localMetadataCollectionId,
                            localServerName,
                            localServerType,
                            localOrganizationName,
                            entityDetail);
                    break;
                case CLASSIFICATION_ADD:
                    instanceSection.setEventType(OMRSInstanceEventType.CLASSIFIED_ENTITY_EVENT);
                    LOG.debug("{}: invoke processClassifiedEntityEvent", methodName);
                    repositoryEventProcessor.processClassifiedEntityEvent(
                            repositoryEventMapperName,
                            localMetadataCollectionId,
                            localServerName,
                            localServerType,
                            localOrganizationName,
                            entityDetail);
                    break;
                case CLASSIFICATION_UPDATE:
                    instanceSection.setEventType(OMRSInstanceEventType.RECLASSIFIED_ENTITY_EVENT);
                    LOG.debug("{}: invoke processReclassifiedEntityEvent", methodName);
                    repositoryEventProcessor.processReclassifiedEntityEvent(
                            repositoryEventMapperName,
                            localMetadataCollectionId,
                            localServerName,
                            localServerType,
                            localOrganizationName,
                            entityDetail);
                    break;
                case CLASSIFICATION_DELETE:
                    instanceSection.setEventType(OMRSInstanceEventType.DECLASSIFIED_ENTITY_EVENT);
                    LOG.debug("{}: invoke processDeclassifiedEntityEvent", methodName);
                    repositoryEventProcessor.processDeclassifiedEntityEvent(
                            repositoryEventMapperName,
                            localMetadataCollectionId,
                            localServerName,
                            localServerType,
                            localOrganizationName,
                            entityDetail);
                    break;
                default:
                    LOG.error("{}: operation type {} not supported", methodName, operationType);
                    instanceSection.setEventType(OMRSInstanceEventType.UNKNOWN_INSTANCE_EVENT);
                    break;
            }


        }

        LOG.debug("{}: processed omrsEventV1={}", methodName, omrsEventV1);

    }

}