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
package org.apache.atlas.openmetadata.adapters.repositoryconnector;


import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;


@Singleton
@Component
public class AtlasStoresProxyImpl implements AtlasStoresProxy {


    private AtlasTypeDefStore typeDefStore;
    private AtlasEntityStore entityStore;
    private AtlasRelationshipStore relationshipStore;
    private EntityDiscoveryService entityDiscoveryService;

    @Inject
    public AtlasStoresProxyImpl(AtlasTypeDefStore      typeDefStore,
                                AtlasEntityStore       entityStore,
                                AtlasRelationshipStore relationshipStore,
                                EntityDiscoveryService entityDiscoveryService) {

        this.entityStore            = entityStore;
        this.typeDefStore           = typeDefStore;
        this.relationshipStore      = relationshipStore;
        this.entityDiscoveryService = entityDiscoveryService;
    }

    public AtlasTypeDefStore getTypeDefStore() {
        return typeDefStore;
    }

    public AtlasEntityStore getEntityStore() {
        return entityStore;
    }

    public AtlasRelationshipStore getRelationshipStore() {
        return relationshipStore;
    }

    public EntityDiscoveryService getEntityDiscoveryService() {
        return entityDiscoveryService;
    }
}