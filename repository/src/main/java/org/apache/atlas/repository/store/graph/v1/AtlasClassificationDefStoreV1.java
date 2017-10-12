/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v1;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassificationDef store in v1 format.
 */
class AtlasClassificationDefStoreV1 extends AtlasAbstractDefStoreV1<AtlasClassificationDef> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasClassificationDefStoreV1.class);

    private static final String  TRAIT_NAME_REGEX   = "[a-zA-Z][a-zA-Z0-9_ .]*";

    private static final Pattern TRAIT_NAME_PATTERN = Pattern.compile(TRAIT_NAME_REGEX);

    public AtlasClassificationDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore, AtlasTypeRegistry typeRegistry) {
        super(typeDefStore, typeRegistry);
    }

    @Override
    public AtlasVertex preCreate(AtlasClassificationDef classificationDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.preCreate({})", classificationDef);
        }

        validateType(classificationDef);

        AtlasType type = typeRegistry.getType(classificationDef.getName());

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.CLASSIFICATION) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, classificationDef.getName(), TypeCategory.TRAIT.name());
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByName(classificationDef.getName());

        if (ret != null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_ALREADY_EXISTS, classificationDef.getName());
        }

        ret = typeDefStore.createTypeVertex(classificationDef);

        updateVertexPreCreate(classificationDef, (AtlasClassificationType)type, ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.preCreate({}): {}", classificationDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasClassificationDef create(AtlasClassificationDef classificationDef, AtlasVertex preCreateResult)
        throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.create({}, {})", classificationDef, preCreateResult);
        }

        AtlasVertex vertex = (preCreateResult == null) ? preCreate(classificationDef) : preCreateResult;

        updateVertexAddReferences(classificationDef, vertex);

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.create({}, {}): {}", classificationDef, preCreateResult, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasClassificationDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.getAll()");
        }

        List<AtlasClassificationDef> ret = new ArrayList<>();

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(TypeCategory.TRAIT);
        while (vertices.hasNext()) {
            ret.add(toClassificationDef(vertices.next()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.getAll(): count={}", ret.size());
        }
        return ret;
    }

    @Override
    public AtlasClassificationDef getByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.getByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, TypeCategory.class);

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.getByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public AtlasClassificationDef getByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.getByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.getByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasClassificationDef update(AtlasClassificationDef classifiDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.update({})", classifiDef);
        }

        validateType(classifiDef);

        AtlasClassificationDef ret = StringUtils.isNotBlank(classifiDef.getGuid())
                  ? updateByGuid(classifiDef.getGuid(), classifiDef) : updateByName(classifiDef.getName(), classifiDef);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.update({}): {}", classifiDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasClassificationDef updateByName(String name, AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.updateByName({}, {})", name, classificationDef);
        }

        validateType(classificationDef);

        AtlasType type = typeRegistry.getType(classificationDef.getName());

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.CLASSIFICATION) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, classificationDef.getName(), TypeCategory.TRAIT.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        updateVertexPreUpdate(classificationDef, (AtlasClassificationType)type, vertex);
        updateVertexAddReferences(classificationDef, vertex);

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.updateByName({}, {}): {}", name, classificationDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasClassificationDef updateByGuid(String guid, AtlasClassificationDef classificationDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.updateByGuid({})", guid);
        }

        validateType(classificationDef);

        AtlasType type = typeRegistry.getTypeByGuid(guid);

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.CLASSIFICATION) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, classificationDef.getName(), TypeCategory.TRAIT.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.TRAIT);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        updateVertexPreUpdate(classificationDef, (AtlasClassificationType)type, vertex);
        updateVertexAddReferences(classificationDef, vertex);

        AtlasClassificationDef ret = toClassificationDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.updateByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasVertex preDeleteByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.preDeleteByName({})", name);
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.TRAIT);

        if (AtlasGraphUtilsV1.typeHasInstanceVertex(name)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_HAS_REFERENCES, name);
        }

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        typeDefStore.deleteTypeVertexOutEdges(ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.preDeleteByName({}): ret=", name, ret);
        }

        return ret;
    }

    @Override
    public AtlasVertex preDeleteByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasClassificationDefStoreV1.preDeleteByGuid({})", guid);
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.TRAIT);

        String typeName = AtlasGraphUtilsV1.getProperty(ret, Constants.TYPENAME_PROPERTY_KEY, String.class);

        if (AtlasGraphUtilsV1.typeHasInstanceVertex(typeName)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_HAS_REFERENCES, typeName);
        }

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        typeDefStore.deleteTypeVertexOutEdges(ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasClassificationDefStoreV1.preDeleteByGuid({}): ret=", guid, ret);
        }

        return ret;
    }

    private void updateVertexPreCreate(AtlasClassificationDef  classificationDef,
                                       AtlasClassificationType classificationType,
                                       AtlasVertex             vertex) throws AtlasBaseException {
        AtlasStructDefStoreV1.updateVertexPreCreate(classificationDef, classificationType, vertex, typeDefStore);
    }

    private void updateVertexPreUpdate(AtlasClassificationDef  classificationDef,
                                       AtlasClassificationType classificationType,
                                       AtlasVertex             vertex) throws AtlasBaseException {
        AtlasStructDefStoreV1.updateVertexPreUpdate(classificationDef, classificationType, vertex, typeDefStore);
    }

    private void updateVertexAddReferences(AtlasClassificationDef classificationDef, AtlasVertex vertex) throws AtlasBaseException {
        AtlasStructDefStoreV1.updateVertexAddReferences(classificationDef, vertex, typeDefStore);

        typeDefStore.createSuperTypeEdges(vertex, classificationDef.getSuperTypes(), TypeCategory.TRAIT);
        // create edges from this vertex to entity Type vertices with the supplied entity type names
        typeDefStore.createEntityTypeEdges(vertex, classificationDef.getEntityTypes());
    }

    private AtlasClassificationDef toClassificationDef(AtlasVertex vertex) throws AtlasBaseException {
        AtlasClassificationDef ret = null;

        if (vertex != null && typeDefStore.isTypeVertex(vertex, TypeCategory.TRAIT)) {
            ret = new AtlasClassificationDef();

            AtlasStructDefStoreV1.toStructDef(vertex, ret, typeDefStore);

            ret.setSuperTypes(typeDefStore.getSuperTypeNames(vertex));
            ret.setEntityTypes(typeDefStore.getEntityTypeNames(vertex));
        }

        return ret;
    }

    @Override
    public boolean isValidName(String typeName) {
        Matcher m = TRAIT_NAME_PATTERN.matcher(typeName);

        return m.matches();
    }
}
