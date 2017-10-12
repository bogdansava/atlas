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
package org.apache.atlas.model.instance;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Reference to an object-instance of AtlasEntity type used in relationship attribute values
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasRelatedObjectId extends AtlasObjectId implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String KEY_RELATIONSHIP_GUID       = "relationshipGuid";
    public static final String KEY_RELATIONSHIP_ATTRIBUTES = "relationshipAttributes";

    private String      displayText             = null;
    private String      relationshipGuid        = null;
    private AtlasStruct relationshipAttributes;

    public AtlasRelatedObjectId() { }

    public AtlasRelatedObjectId(String guid, String typeName, String relationshipGuid, AtlasStruct relationshipAttributes) {
        super(guid, typeName);

        setRelationshipGuid(relationshipGuid);
        setRelationshipAttributes(relationshipAttributes);
    }

    public AtlasRelatedObjectId(String guid, String typeName, Map<String, Object> uniqueAttributes, String displayText,
                                String relationshipGuid, AtlasStruct relationshipAttributes) {
        super(guid, typeName, uniqueAttributes);

        setRelationshipGuid(relationshipGuid);
        setDisplayText(displayText);
        setRelationshipAttributes(relationshipAttributes);
    }

    public String getDisplayText() { return displayText; }

    public void setDisplayText(String displayText) { this.displayText = displayText; }

    public String getRelationshipGuid() { return relationshipGuid; }

    public void setRelationshipGuid(String relationshipGuid) { this.relationshipGuid = relationshipGuid; }

    public AtlasStruct getRelationshipAttributes() { return relationshipAttributes; }

    public void setRelationshipAttributes(AtlasStruct relationshipAttributes) { this.relationshipAttributes = relationshipAttributes; }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        AtlasRelatedObjectId that = (AtlasRelatedObjectId) o;
        return Objects.equals(displayText, that.displayText) &&
               Objects.equals(relationshipGuid, that.relationshipGuid) &&
               Objects.equals(relationshipAttributes, that.relationshipAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), displayText, relationshipGuid, relationshipAttributes);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Override
    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasRelatedObjectId{");
        super.toString(sb);
        sb.append("displayText='").append(displayText).append('\'');
        sb.append(", relationshipGuid='").append(relationshipGuid).append('\'');
        sb.append(", relationshipAttributes=").append(relationshipAttributes);
        sb.append('}');

        return sb;
    }
}