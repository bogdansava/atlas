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

package org.apache.atlas;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.TypesChangeListener;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.audit.InMemoryEntityAuditRepository;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedMetadataRepository;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.repository.typestore.GraphBackedTypeStore;
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.services.DefaultMetadataService;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.IInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.*;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.cache.DefaultTypeCache;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.testng.Assert;
import org.testng.SkipException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.*;
import static org.testng.Assert.assertEquals;

/**
 * Test utility class.
 */
public final class TestUtils {

    public static final long TEST_DATE_IN_LONG = 1418265358440L;


    public static final String EMPLOYEES_ATTR = "employees";
    public static final String DEPARTMENT_ATTR = "department";
    public static final String ASSETS_ATTR = "assets";

    public static final String POSITIONS_ATTR = "positions";
    public static final String ASSET_TYPE = "TestAsset";

    public static final String DATABASE_TYPE = "hive_database";
    public static final String DATABASE_NAME = "foo";
    public static final String TABLE_TYPE = "hive_table";
    public static final String PROCESS_TYPE = "hive_process";
    public static final String COLUMN_TYPE = "column_type";
    public static final String TABLE_NAME = "bar";
    public static final String CLASSIFICATION = "classification";
    public static final String PII = "PII";
    public static final String SUPER_TYPE_NAME = "Base";
    public static final String STORAGE_DESC_TYPE = "hive_storagedesc";
    public static final String PARTITION_STRUCT_TYPE = "partition_struct_type";
    public static final String PARTITION_CLASS_TYPE = "partition_class_type";
    public static final String SERDE_TYPE = "serdeType";
    public static final String COLUMNS_MAP = "columnsMap";
    public static final String COLUMNS_ATTR_NAME = "columns";

    public static final String NAME = "name";

    private TestUtils() {
    }

    /**
     * Dumps the graph in GSON format in the path returned.
     *
     * @param graph handle to graph
     * @return path to the dump file
     * @throws Exception
     */
    public static String dumpGraph(AtlasGraph<?,?> graph) throws Exception {
        File tempFile = File.createTempFile("graph", ".gson");
        System.out.println("tempFile.getPath() = " + tempFile.getPath());
        GraphHelper.dumpToLog(graph);
        FileOutputStream os = null;
        try {
            os = new FileOutputStream(tempFile);
            graph.exportToGson(os);
        }
        finally {
            if(os != null) {
                try {
                    os.close();
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return tempFile.getPath();
    }

    /**
     * Class Hierarchy is:
     * Department(name : String, employees : Array[Person])
     * Person(name : String, department : Department, manager : Manager)
     * Manager(subordinates : Array[Person]) extends Person
     * <p/>
     * Persons can have SecurityClearance(level : Int) clearance.
     */
    public static void defineDeptEmployeeTypes(TypeSystem ts) throws AtlasException {

        String _description = "_description";
        EnumTypeDefinition orgLevelEnum =
                new EnumTypeDefinition("OrgLevel", "OrgLevel"+_description, new EnumValue("L1", 1), new EnumValue("L2", 2));

        StructTypeDefinition addressDetails =
                createStructTypeDef("Address", "Address"+_description, createRequiredAttrDef("street", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("city", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> deptTypeDef = createClassTypeDef(DEPARTMENT_TYPE, "Department"+_description, ImmutableSet.<String>of(),
                createRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                new AttributeDefinition(EMPLOYEES_ATTR, String.format("array<%s>", "Person"), Multiplicity.OPTIONAL,
                        true, DEPARTMENT_ATTR),
                new AttributeDefinition(POSITIONS_ATTR, String.format("map<%s,%s>", DataTypes.STRING_TYPE.getName(), "Person"), Multiplicity.OPTIONAL,
                        false, null)
                );

        HierarchicalTypeDefinition<ClassType> personTypeDef = createClassTypeDef("Person", "Person"+_description, ImmutableSet.<String>of(),
                createRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                createOptionalAttrDef("orgLevel", "OrgLevel"),
                createOptionalAttrDef("address", "Address"),
                new AttributeDefinition(DEPARTMENT_ATTR, "Department", Multiplicity.REQUIRED, false, EMPLOYEES_ATTR),
                new AttributeDefinition("manager", "Manager", Multiplicity.OPTIONAL, false, "subordinates"),
                new AttributeDefinition("mentor", "Person", Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(ASSETS_ATTR, String.format("array<%s>", ASSET_TYPE) , Multiplicity.OPTIONAL, false, null),
                createOptionalAttrDef("birthday", DataTypes.DATE_TYPE),
                createOptionalAttrDef("hasPets", DataTypes.BOOLEAN_TYPE),
                createOptionalAttrDef("numberOfCars", DataTypes.BYTE_TYPE),
                createOptionalAttrDef("houseNumber", DataTypes.SHORT_TYPE),
                createOptionalAttrDef("carMileage", DataTypes.INT_TYPE),
                createOptionalAttrDef("shares", DataTypes.LONG_TYPE),
                createOptionalAttrDef("salary", DataTypes.DOUBLE_TYPE),
                createOptionalAttrDef("age", DataTypes.FLOAT_TYPE),
                createOptionalAttrDef("numberOfStarsEstimate", DataTypes.BIGINTEGER_TYPE),
                createOptionalAttrDef("approximationOfPi", DataTypes.BIGDECIMAL_TYPE),
                createOptionalAttrDef("isOrganDonor", DataTypes.BOOLEAN_TYPE)
                );


        HierarchicalTypeDefinition<ClassType> assetTypeDef = createClassTypeDef(ASSET_TYPE, "Asset"+_description, ImmutableSet.<String>of(),
                createRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                new AttributeDefinition("childAssets", String.format("array<%s>", ASSET_TYPE) , Multiplicity.OPTIONAL, false, null)
                );

        HierarchicalTypeDefinition<ClassType> managerTypeDef = createClassTypeDef("Manager", "Manager"+_description, ImmutableSet.of("Person"),
                new AttributeDefinition("subordinates", String.format("array<%s>", "Person"), Multiplicity.COLLECTION,
                        false, "manager"));

        HierarchicalTypeDefinition<TraitType> securityClearanceTypeDef =
                createTraitTypeDef("SecurityClearance", "SecurityClearance"+_description, ImmutableSet.<String>of(),
                        createRequiredAttrDef("level", DataTypes.INT_TYPE));

        ts.defineTypes(ImmutableList.of(orgLevelEnum), ImmutableList.of(addressDetails),
                ImmutableList.of(securityClearanceTypeDef),
                ImmutableList.of(deptTypeDef, personTypeDef, managerTypeDef, assetTypeDef));
    }

    public static final String DEPARTMENT_TYPE = "Department";
    public static final String PERSON_TYPE = "Person";

    public static ITypedReferenceableInstance createDeptEg1(TypeSystem ts) throws AtlasException {
        Referenceable hrDept = new Referenceable(DEPARTMENT_TYPE);
        Referenceable john = new Referenceable(PERSON_TYPE);

        Referenceable jane = new Referenceable("Manager", "SecurityClearance");
        Referenceable johnAddr = new Referenceable("Address");
        Referenceable janeAddr = new Referenceable("Address");
        Referenceable julius = new Referenceable("Manager");
        Referenceable juliusAddr = new Referenceable("Address");
        Referenceable max = new Referenceable("Person");
        Referenceable maxAddr = new Referenceable("Address");

        hrDept.set(NAME, "hr");
        john.set(NAME, "John");
        john.set(DEPARTMENT_ATTR, hrDept);
        johnAddr.set("street", "Stewart Drive");
        johnAddr.set("city", "Sunnyvale");
        john.set("address", johnAddr);

        john.set("birthday",new Date(1950, 5, 15));
        john.set("isOrganDonor", true);
        john.set("hasPets", true);
        john.set("numberOfCars", 1);
        john.set("houseNumber", 153);
        john.set("carMileage", 13364);
        john.set("shares", 15000);
        john.set("salary", 123345.678);
        john.set("age", 50);
        john.set("numberOfStarsEstimate", new BigInteger("1000000000000000000000"));
        john.set("approximationOfPi", new BigDecimal("3.141592653589793238462643383279502884197169399375105820974944592307816406286"));

        jane.set(NAME, "Jane");
        jane.set(DEPARTMENT_ATTR, hrDept);
        janeAddr.set("street", "Great America Parkway");
        janeAddr.set("city", "Santa Clara");
        jane.set("address", janeAddr);
        janeAddr.set("street", "Great America Parkway");

        julius.set(NAME, "Julius");
        julius.set(DEPARTMENT_ATTR, hrDept);
        juliusAddr.set("street", "Madison Ave");
        juliusAddr.set("city", "Newtonville");
        julius.set("address", juliusAddr);
        julius.set("subordinates", ImmutableList.<Referenceable>of());

        max.set(NAME, "Max");
        max.set(DEPARTMENT_ATTR, hrDept);
        maxAddr.set("street", "Ripley St");
        maxAddr.set("city", "Newton");
        max.set("address", maxAddr);
        max.set("manager", jane);
        max.set("mentor", julius);
        max.set("birthday",new Date(1979, 3, 15));
        max.set("isOrganDonor", true);
        max.set("hasPets", true);
        max.set("age", 36);
        max.set("numberOfCars", 2);
        max.set("houseNumber", 17);
        max.set("carMileage", 13);
        max.set("shares", Long.MAX_VALUE);
        max.set("salary", Double.MAX_VALUE);
        max.set("numberOfStarsEstimate", new BigInteger("1000000000000000000000000000000"));
        max.set("approximationOfPi", new BigDecimal("3.1415926535897932"));

        john.set("manager", jane);
        john.set("mentor", max);
        hrDept.set(EMPLOYEES_ATTR, ImmutableList.of(john, jane, julius, max));

        jane.set("subordinates", ImmutableList.of(john, max));

        jane.getTrait("SecurityClearance").set("level", 1);

        ClassType deptType = ts.getDataType(ClassType.class, "Department");
        ITypedReferenceableInstance hrDept2 = deptType.convert(hrDept, Multiplicity.REQUIRED);
        Assert.assertNotNull(hrDept2);

        return hrDept2;
    }



    public static TypesDef simpleType(){
        HierarchicalTypeDefinition<ClassType> superTypeDefinition =
                createClassTypeDef("h_type", ImmutableSet.<String>of(),
                        createOptionalAttrDef("attr", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("s_type", "structType",
                new AttributeDefinition[]{createRequiredAttrDef(NAME, DataTypes.STRING_TYPE)});

        HierarchicalTypeDefinition<TraitType> traitTypeDefinition =
                createTraitTypeDef("t_type", "traitType", ImmutableSet.<String>of());

        EnumValue values[] = {new EnumValue("ONE", 1),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("e_type", "enumType", values);
        return TypesUtil.getTypesDef(ImmutableList.of(enumTypeDefinition), ImmutableList.of(structTypeDefinition),
                ImmutableList.of(traitTypeDefinition), ImmutableList.of(superTypeDefinition));
    }

    public static TypesDef simpleTypeUpdated(){
        HierarchicalTypeDefinition<ClassType> superTypeDefinition =
                createClassTypeDef("h_type", ImmutableSet.<String>of(),
                        createOptionalAttrDef("attr", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> newSuperTypeDefinition =
                createClassTypeDef("new_h_type", ImmutableSet.<String>of(),
                        createOptionalAttrDef("attr", DataTypes.STRING_TYPE));

        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("s_type", "structType",
                new AttributeDefinition[]{createRequiredAttrDef(NAME, DataTypes.STRING_TYPE)});

        HierarchicalTypeDefinition<TraitType> traitTypeDefinition =
                createTraitTypeDef("t_type", "traitType", ImmutableSet.<String>of());

        EnumValue values[] = {new EnumValue("ONE", 1),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("e_type", "enumType", values);
        return TypesUtil.getTypesDef(ImmutableList.of(enumTypeDefinition), ImmutableList.of(structTypeDefinition),
                ImmutableList.of(traitTypeDefinition), ImmutableList.of(superTypeDefinition, newSuperTypeDefinition));
    }

    public static TypesDef simpleTypeUpdatedDiff() {
        HierarchicalTypeDefinition<ClassType> newSuperTypeDefinition =
                createClassTypeDef("new_h_type", ImmutableSet.<String>of(),
                        createOptionalAttrDef("attr", DataTypes.STRING_TYPE));

        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(), ImmutableList.of(newSuperTypeDefinition));
    }

    public static TypesDef defineHiveTypes() {
        String _description = "_description";
        HierarchicalTypeDefinition<ClassType> superTypeDefinition =
                createClassTypeDef(SUPER_TYPE_NAME, ImmutableSet.<String>of(),
                        createOptionalAttrDef("namespace", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("cluster", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("colo", DataTypes.STRING_TYPE));
        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef(DATABASE_TYPE, DATABASE_TYPE + _description,ImmutableSet.of(SUPER_TYPE_NAME),
                        TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                        createOptionalAttrDef("created", DataTypes.DATE_TYPE),
                        createOptionalAttrDef("isReplicated", DataTypes.BOOLEAN_TYPE),
                        new AttributeDefinition("parameters", new DataTypes.MapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE).getName(), Multiplicity.OPTIONAL, false, null),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE));


        StructTypeDefinition structTypeDefinition = new StructTypeDefinition("serdeType", "serdeType" + _description,
                new AttributeDefinition[]{createRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                    createRequiredAttrDef("serde", DataTypes.STRING_TYPE),
                    createOptionalAttrDef("description", DataTypes.STRING_TYPE)});

        EnumValue values[] = {new EnumValue("MANAGED", 1), new EnumValue("EXTERNAL", 2),};

        EnumTypeDefinition enumTypeDefinition = new EnumTypeDefinition("tableType", "tableType" + _description, values);

        HierarchicalTypeDefinition<ClassType> columnsDefinition =
                createClassTypeDef(COLUMN_TYPE, ImmutableSet.<String>of(),
                        createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE));

        StructTypeDefinition partitionDefinition = new StructTypeDefinition("partition_struct_type", "partition_struct_type" + _description,
                new AttributeDefinition[]{createRequiredAttrDef(NAME, DataTypes.STRING_TYPE),});

        AttributeDefinition[] attributeDefinitions = new AttributeDefinition[]{
            new AttributeDefinition("location", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("inputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("outputFormat", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("compressed", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.REQUIRED, false,
                null),
            new AttributeDefinition("numBuckets", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            };

        HierarchicalTypeDefinition<ClassType> storageDescClsDef =
            new HierarchicalTypeDefinition<>(ClassType.class, STORAGE_DESC_TYPE, STORAGE_DESC_TYPE + _description,
                ImmutableSet.of(SUPER_TYPE_NAME), attributeDefinitions);

        AttributeDefinition[] partClsAttributes = new AttributeDefinition[]{
            new AttributeDefinition("values", DataTypes.arrayTypeName(DataTypes.STRING_TYPE.getName()),
                Multiplicity.OPTIONAL, false, null),
            new AttributeDefinition("table", TABLE_TYPE, Multiplicity.REQUIRED, false, null),
            new AttributeDefinition("createTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("lastAccessTime", DataTypes.LONG_TYPE.getName(), Multiplicity.OPTIONAL, false,
                null),
            new AttributeDefinition("sd", STORAGE_DESC_TYPE, Multiplicity.REQUIRED, true,
                null),
            new AttributeDefinition("columns", DataTypes.arrayTypeName(COLUMN_TYPE),
                Multiplicity.OPTIONAL, true, null),
            new AttributeDefinition("parameters", new DataTypes.MapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE).getName(), Multiplicity.OPTIONAL, false, null),};

        HierarchicalTypeDefinition<ClassType> partClsDef =
            new HierarchicalTypeDefinition<>(ClassType.class, "partition_class_type", "partition_class_type" + _description,
                ImmutableSet.of(SUPER_TYPE_NAME), partClsAttributes);

        HierarchicalTypeDefinition<ClassType> processClsType =
                new HierarchicalTypeDefinition<>(ClassType.class, PROCESS_TYPE, PROCESS_TYPE + _description,
                        ImmutableSet.<String>of(), new AttributeDefinition[]{
                        new AttributeDefinition("outputs", "array<" + TABLE_TYPE + ">", Multiplicity.OPTIONAL, false, null)
                });

        HierarchicalTypeDefinition<ClassType> tableTypeDefinition =
                createClassTypeDef(TABLE_TYPE, TABLE_TYPE + _description, ImmutableSet.of(SUPER_TYPE_NAME),
                        TypesUtil.createUniqueRequiredAttrDef(NAME, DataTypes.STRING_TYPE),
                        createRequiredAttrDef("description", DataTypes.STRING_TYPE),
                        createRequiredAttrDef("type", DataTypes.STRING_TYPE),
                        createOptionalAttrDef("created", DataTypes.DATE_TYPE),
                        // enum
                        new AttributeDefinition("tableType", "tableType", Multiplicity.REQUIRED, false, null),
                        // array of strings
                        new AttributeDefinition("columnNames",
                                String.format("array<%s>", DataTypes.STRING_TYPE.getName()), Multiplicity.OPTIONAL,
                                false, null),
                        // array of classes
                        new AttributeDefinition("columns", String.format("array<%s>", COLUMN_TYPE),
                                Multiplicity.OPTIONAL, true, null),
                        // array of structs
                        new AttributeDefinition("partitions", String.format("array<%s>", "partition_struct_type"),
                                Multiplicity.OPTIONAL, true, null),
                        // map of primitives
                        new AttributeDefinition("parametersMap",
                                DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE.getName()),
                                Multiplicity.OPTIONAL, true, null),
                        //map of classes -
                        new AttributeDefinition(COLUMNS_MAP,
                                                        DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                                                                COLUMN_TYPE),
                                                        Multiplicity.OPTIONAL, true, null),
                         //map of structs
                        new AttributeDefinition("partitionsMap",
                                                        DataTypes.mapTypeName(DataTypes.STRING_TYPE.getName(),
                                                                "partition_struct_type"),
                                                        Multiplicity.OPTIONAL, true, null),
                        // struct reference
                        new AttributeDefinition("serde1", "serdeType", Multiplicity.OPTIONAL, false, null),
                        new AttributeDefinition("serde2", "serdeType", Multiplicity.OPTIONAL, false, null),
                        // class reference
                        new AttributeDefinition("database", DATABASE_TYPE, Multiplicity.REQUIRED, false, null),
                        //class reference as composite
                        new AttributeDefinition("databaseComposite", DATABASE_TYPE, Multiplicity.OPTIONAL, true, null));

        HierarchicalTypeDefinition<TraitType> piiTypeDefinition =
                createTraitTypeDef(PII, PII + _description, ImmutableSet.<String>of());

        HierarchicalTypeDefinition<TraitType> classificationTypeDefinition =
                createTraitTypeDef(CLASSIFICATION, CLASSIFICATION + _description, ImmutableSet.<String>of(),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<TraitType> fetlClassificationTypeDefinition =
                createTraitTypeDef("fetl" + CLASSIFICATION, "fetl" + CLASSIFICATION + _description, ImmutableSet.of(CLASSIFICATION),
                        createRequiredAttrDef("tag", DataTypes.STRING_TYPE));

        return TypesUtil.getTypesDef(ImmutableList.of(enumTypeDefinition),
                ImmutableList.of(structTypeDefinition, partitionDefinition),
                ImmutableList.of(classificationTypeDefinition, fetlClassificationTypeDefinition, piiTypeDefinition),
                ImmutableList.of(superTypeDefinition, databaseTypeDefinition, columnsDefinition, tableTypeDefinition,
                        storageDescClsDef, partClsDef, processClsType));
    }

    public static Collection<IDataType> createHiveTypes(TypeSystem typeSystem) throws Exception {
        if (!typeSystem.isRegistered(TABLE_TYPE)) {
            TypesDef typesDef = defineHiveTypes();
            return typeSystem.defineTypes(typesDef).values();
        }
        return new ArrayList<>();
    }

    public static final String randomString() {
        return randomString(10);
    }

    public static final String randomString(int count) {
        final String prefix = "r";

        return prefix + RandomStringUtils.randomAlphanumeric(count - prefix.length()); // ensure that the string starts with a letter
    }

    public static Referenceable createDBEntity() {
        Referenceable entity = new Referenceable(DATABASE_TYPE);
        String dbName = RandomStringUtils.randomAlphanumeric(10);
        entity.set(NAME, dbName);
        entity.set("description", "us db");
        return entity;
    }

    public static Referenceable createTableEntity(String dbId) {
        Referenceable entity = new Referenceable(TABLE_TYPE);
        String tableName = RandomStringUtils.randomAlphanumeric(10);
        entity.set(NAME, tableName);
        entity.set("description", "random table");
        entity.set("type", "type");
        entity.set("tableType", "MANAGED");
        entity.set("database", new Id(dbId, 0, DATABASE_TYPE));
        entity.set("created", new Date());
        return entity;
    }

    public static Referenceable createColumnEntity() {
        Referenceable entity = new Referenceable(COLUMN_TYPE);
        entity.set(NAME, RandomStringUtils.randomAlphanumeric(10));
        entity.set("type", "VARCHAR(32)");
        return entity;
    }

    /**
     * Creates an entity in the graph and does basic validation
     * of the GuidMapping that was created in the process.
     *
     */
    public static String createInstance(MetadataService metadataService, Referenceable entity) throws Exception {
        RequestContext.createContext();

        String entityjson = InstanceSerialization.toJson(entity, true);
        JSONArray entitiesJson = new JSONArray();
        entitiesJson.put(entityjson);
        CreateUpdateEntitiesResult creationResult    = metadataService.createEntities(entitiesJson.toString());
        Map<String,String>         guidMap           = creationResult.getGuidMapping().getGuidAssignments();
        Map<Id, Referenceable>     referencedObjects = findReferencedObjects(entity);

        for(Map.Entry<Id,Referenceable> entry : referencedObjects.entrySet()) {
            Id foundId = entry.getKey();
            if(foundId.isUnassigned()) {
                String guid = guidMap.get(entry.getKey()._getId());
                Referenceable obj = entry.getValue();
                loadAndDoSimpleValidation(guid,obj, metadataService);
            }
        }
        List<String> guids = creationResult.getCreatedEntities();
        if (guids != null && guids.size() > 0) {
            return guids.get(guids.size() - 1);
        }
        return null;
    }

    private static Map<Id,Referenceable> findReferencedObjects(Referenceable ref) {
        Map<Id, Referenceable> result = new HashMap<>();
        findReferencedObjects(ref, result);
        return result;
    }

    private static void findReferencedObjects(Referenceable ref, Map<Id, Referenceable> seen) {

        Id guid = ref.getId();
        if(seen.containsKey(guid)) {
            return;
        }
        seen.put(guid, ref);
        for(Map.Entry<String, Object> attr : ref.getValuesMap().entrySet()) {
            Object value = attr.getValue();
            if(value instanceof Referenceable) {
                findReferencedObjects((Referenceable)value, seen);
            }
            else if(value instanceof List) {
                for(Object o : (List)value) {
                    if(o instanceof Referenceable) {
                        findReferencedObjects((Referenceable)o, seen);
                    }
                }
            }
            else if(value instanceof Map) {
                for(Object o : ((Map)value).values()) {
                    if(o instanceof Referenceable) {
                        findReferencedObjects((Referenceable)o, seen);
                    }
                }
            }
        }
    }

    /**
     * Clears the state in the request context.
     *
     */
    public static void resetRequestContext() {
        //reset the context while preserving the user
        String user = RequestContext.get().getUser();
        RequestContext.createContext();
        RequestContext.get().setUser(user);
    }

    /**
     * Triggers the Atlas initialization process using the specified MetadataRepository.
     * This causes the built-in types and their indices to be created.
     */
    public static void setupGraphProvider(MetadataRepository repo) throws AtlasException {
        TypeCache typeCache = null;
        try {
            typeCache = AtlasRepositoryConfiguration.getTypeCache().newInstance();
        }
        catch(Throwable t) {
            typeCache = new DefaultTypeCache();
        }
        final GraphBackedSearchIndexer indexer = new GraphBackedSearchIndexer(new AtlasTypeRegistry());

        Configuration config = ApplicationProperties.get();
        ITypeStore typeStore = new GraphBackedTypeStore(AtlasGraphProvider.getGraphInstance());
        DefaultMetadataService defaultMetadataService = new DefaultMetadataService(repo,
                typeStore,
                new HashSet<TypesChangeListener>() {{ add(indexer); }},
                new HashSet<EntityChangeListener>(),
                TypeSystem.getInstance(),
                config,
                typeCache,
                // Fixme: Can we work with Noop
                new InMemoryEntityAuditRepository());

        //commit the created types
        getGraph().commit();

    }

    public static AtlasGraph getGraph() {

        return AtlasGraphProvider.getGraphInstance();

    }

    /**
     * Adds a proxy wrapper around the specified MetadataService that automatically
     * resets the request context before every call.
     *
     * @param delegate
     * @return
     */
    public static MetadataService addSessionCleanupWrapper(final MetadataService delegate) {

        return (MetadataService)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class[]{MetadataService.class}, new InvocationHandler() {

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

                try {
                    resetRequestContext();
                    Object result = method.invoke(delegate, args);

                    return result;
                }
                catch(InvocationTargetException e) {
                    e.getCause().printStackTrace();
                    throw e.getCause();
                }
                catch(Throwable t) {
                    t.printStackTrace();
                    throw t;
                }
            }

        });
    }

    /**
     * Adds a proxy wrapper around the specified MetadataRepository that automatically
     * resets the request context before every call and either commits or rolls
     * back the graph transaction after every call.
     *
     * @param delegate
     * @return
     */
    public static MetadataRepository addTransactionWrapper(final MetadataRepository delegate) {
        return (MetadataRepository)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class[]{MetadataRepository.class}, new InvocationHandler() {

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                boolean useTransaction = GraphBackedMetadataRepository.class.getMethod(
                        method.getName(), method.getParameterTypes())
                        .isAnnotationPresent(GraphTransaction.class);
                try {
                    resetRequestContext();
                    Object result = method.invoke(delegate, args);
                    if(useTransaction) {
                        System.out.println("Committing changes");
                        getGraph().commit();
                        System.out.println("Commit succeeded.");
                    }
                    return result;
                }
                catch(InvocationTargetException e) {
                    e.getCause().printStackTrace();
                    if(useTransaction) {
                        System.out.println("Rolling back changes due to exception.");
                        getGraph().rollback();
                    }
                    throw e.getCause();
                }
                catch(Throwable t) {
                    t.printStackTrace();
                    if(useTransaction) {
                        System.out.println("Rolling back changes due to exception.");
                        getGraph().rollback();
                    }
                    throw t;
                }
            }

        });
    }

    /**
     * Loads the entity and does sanity testing of the GuidMapping  that was
     * created during the operation.
     *
     */
    public static ITypedReferenceableInstance loadAndDoSimpleValidation(String guid, Referenceable original, MetadataRepository repositoryService) throws AtlasException {
        ITypedReferenceableInstance loaded = repositoryService.getEntityDefinition(guid);
        doSimpleValidation(original,  loaded);
        return loaded;
    }

    /**
     * Loads the entity and does sanity testing of the GuidMapping that was
     * created during the operation.
     *
     */
    public static ITypedReferenceableInstance loadAndDoSimpleValidation(String guid, Referenceable original, MetadataService repositoryService) throws AtlasException {
        ITypedReferenceableInstance loaded = repositoryService.getEntityDefinition(guid);
        doSimpleValidation(original,  loaded);
        return loaded;

    }

    private static void doSimpleValidation(Referenceable original, IInstance loaded) throws AtlasException {

        assertEquals(loaded.getTypeName(), original.getTypeName());
        ClassType ct = TypeSystem.getInstance().getDataType(ClassType.class, loaded.getTypeName());

        //compare primitive fields
        for(AttributeInfo field : ct.fieldMapping.fields.values()) {
            if(field.dataType().getTypeCategory() == TypeCategory.PRIMITIVE) {
                if(original.get(field.name) != null) {
                    Object rawLoadedValue = loaded.get(field.name);
                    Object rawProvidedValue = original.get(field.name);
                    Object convertedLoadedValue = field.dataType().convert(rawLoadedValue, Multiplicity.REQUIRED);
                    Object convertedProvidedValue = field.dataType().convert(rawProvidedValue, Multiplicity.REQUIRED);

                    assertEquals(convertedLoadedValue, convertedProvidedValue);
                }
            }
        }
    }

    /**
     * Validates that the two String Collections contain the same items, without
     * regard to order.
     *
     */
    public static void assertContentsSame(Collection<String> actual, Collection<String> expected) {
        assertEquals(actual.size(), expected.size());
        Set<String> checker = new HashSet<>();
        checker.addAll(expected);
        checker.removeAll(actual);
        assertEquals(checker.size(), 0);
    }

    public static void skipForGremlin3EnabledGraphDb() throws SkipException {
        //ATLAS-1579 Currently, some tests are skipped for titan1 backened. As these tests are hard coded to use Gremlin2. See ATLAS-1579, ATLAS-1591 once it is fixed, please remove it.
         if (TestUtils.getGraph().getSupportedGremlinVersion() == GremlinVersion.THREE) {
             throw new SkipException ("This test requires Gremlin2. Skipping test ");
         }
    }

}
