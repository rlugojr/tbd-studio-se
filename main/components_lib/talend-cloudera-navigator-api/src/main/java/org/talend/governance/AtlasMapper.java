package org.talend.governance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.*;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONArray;
import org.talend.cloudera.navigator.api.NavigatorNode;

import java.util.*;

/**
 * Class to map the Talend DAG into an Atlas DAG
 */
public class AtlasMapper {

    static final String DATABASE_TYPE = "DB";
    static final String COLUMN_TYPE = "Column";
    static final String TABLE_TYPE = "Table";
    static final String VIEW_TYPE = "View";
    static final String LOAD_PROCESS_TYPE = "LoadProcess";
    static final String STORAGE_DESC_TYPE = "StorageDesc";

    public static final String DB_ATTRIBUTE = "db";
    public static final String COLUMNS_ATTRIBUTE = "columns";

    private String ENDPOINT_URL = "http://localhost:21000";
    private AtlasClient client =  new AtlasClient(ENDPOINT_URL);


    /**
     * TODO this function must return the appropriate type of the objects that are going to be persisted
     * @param navigatorNodes
     * @param jobId
     */
    public void map(List<NavigatorNode> navigatorNodes, String jobId) {
        // First create the type definition
        TypesDef typesDef = createTypeDefinitions();
//        List<String> ids = persist(typesDef);
//        Collection<String> types = getTypes();
//        System.out.println(types);

        // we create the Entities
        Collection<Referenceable> refs = new ArrayList<>();
        for (NavigatorNode navigatorNode : navigatorNodes) {
            Referenceable ref = createEntity(navigatorNode);
            Id id = createInstance(ref);
            refs.add(ref);
        }
        persist(refs);

        // verify types created
//        verifyTypesCreated();
    }

    private Collection<String> getTypes() {
        Collection<String> types = new ArrayList<>();
        try {
            List<String> existingTypesDef = client.listTypes();
//            System.out.println(existingTypesDef);
            for (String existingTypeDef: existingTypesDef) {
                String type = client.getType(existingTypeDef);
                types.add(type);
            }
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return types;
    }

    public List<String> persist(TypesDef typesDef) {
//        System.out.println("typesAsJSON = " + TypesSerialization.toJson(typesDef));
        List<String> ids = new ArrayList<>();
        try {
            ids = client.createType(typesDef);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return ids;
    }

    private Id createInstance(Referenceable referenceable) {
        String entityJSON = InstanceSerialization.toJson(referenceable, true);
        System.out.println("Submitting new entity= " + entityJSON);
        JSONArray guids = null;
        try {
            guids = client.createEntity(entityJSON);
            String typeName = referenceable.getTypeName();
            System.out.println("created instance for type " + typeName + ", guid: " + guids);
            // return the Id for created instance with guid
            return new Id(guids.getString(guids.length()-1), referenceable.getId().getVersion(),
                    referenceable.getTypeName());
//        } catch (AtlasServiceException e) {
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    public JSONArray persist(Collection<Referenceable> refs) {
//        for (Referenceable ref : refs) {
//            System.out.println(InstanceSerialization.toJson(ref, true));
//        }
        JSONArray jsonArray = new JSONArray();
        try {
            jsonArray = client.createEntity(refs);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return jsonArray;
    }


    /**
     *
     * @return
     */
    private TypesDef createTypeDefinitions() {
        ImmutableList<EnumTypeDefinition> enums = ImmutableList.<EnumTypeDefinition>of();
        ImmutableList<StructTypeDefinition> structs = ImmutableList.<StructTypeDefinition>of();

        HierarchicalTypeDefinition<TraitType> dimTraitDef = TypesUtil.createTraitTypeDef("Dimension",  "Dimension Trait", null);
        HierarchicalTypeDefinition<TraitType> factTraitDef = TypesUtil.createTraitTypeDef("Fact", "Fact Trait", null);
        HierarchicalTypeDefinition<TraitType> piiTraitDef = TypesUtil.createTraitTypeDef("PII", "PII Trait", null);
        HierarchicalTypeDefinition<TraitType> metricTraitDef = TypesUtil.createTraitTypeDef("Metric", "Metric Trait", null);
        HierarchicalTypeDefinition<TraitType> etlTraitDef = TypesUtil.createTraitTypeDef("ETL", "ETL Trait", null);
        HierarchicalTypeDefinition<TraitType> jdbcTraitDef = TypesUtil.createTraitTypeDef("JdbcAccess", "JdbcAccess Trait", null);
        HierarchicalTypeDefinition<TraitType> logTraitDef = TypesUtil.createTraitTypeDef("Log Data", "LogData Trait",  null);
        ImmutableList<HierarchicalTypeDefinition<TraitType>> traits =
                ImmutableList.of(dimTraitDef, factTraitDef, piiTraitDef, metricTraitDef,
                        etlTraitDef, jdbcTraitDef, logTraitDef);

        HierarchicalTypeDefinition<ClassType> dbClsDef = TypesUtil
                .createClassTypeDef(DATABASE_TYPE, DATABASE_TYPE, null,
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("description", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("locationUri", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("owner", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("createTime", DataTypes.LONG_TYPE));

        HierarchicalTypeDefinition<ClassType> storageDescClsDef = TypesUtil
                .createClassTypeDef(STORAGE_DESC_TYPE, STORAGE_DESC_TYPE, null,
                        TypesUtil.createOptionalAttrDef("location", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("inputFormat", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("outputFormat", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("compressed", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> columnClsDef = TypesUtil
                .createClassTypeDef(COLUMN_TYPE, COLUMN_TYPE, null,
                        TypesUtil.createOptionalAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("dataType", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("comment", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> tblClsDef = TypesUtil
                .createClassTypeDef(TABLE_TYPE, TABLE_TYPE, ImmutableSet.of("DataSet"),
                        TypesUtil.createRequiredAttrDef(DB_ATTRIBUTE, DATABASE_TYPE),
                        new AttributeDefinition("sd", STORAGE_DESC_TYPE, Multiplicity.REQUIRED, true, null),
                        TypesUtil.createOptionalAttrDef("owner", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("createTime", DataTypes.LONG_TYPE),
                        TypesUtil.createOptionalAttrDef("lastAccessTime", DataTypes.LONG_TYPE),
                        TypesUtil.createOptionalAttrDef("retention", DataTypes.LONG_TYPE),
                        TypesUtil.createOptionalAttrDef("viewOriginalText", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("viewExpandedText", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("tableType", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("temporary", DataTypes.BOOLEAN_TYPE),
                        new AttributeDefinition(COLUMNS_ATTRIBUTE, DataTypes.arrayTypeName(COLUMN_TYPE),
                                Multiplicity.COLLECTION, true, null));

        HierarchicalTypeDefinition<ClassType> loadProcessClsDef = TypesUtil
                .createClassTypeDef(LOAD_PROCESS_TYPE, LOAD_PROCESS_TYPE, ImmutableSet.of("Process"),
                        TypesUtil.createOptionalAttrDef("userName", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("startTime", DataTypes.LONG_TYPE),
                        TypesUtil.createOptionalAttrDef("endTime", DataTypes.LONG_TYPE),
                        TypesUtil.createRequiredAttrDef("queryText", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("queryPlan", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("queryId", DataTypes.STRING_TYPE),
                        TypesUtil.createRequiredAttrDef("queryGraph", DataTypes.STRING_TYPE));

        HierarchicalTypeDefinition<ClassType> viewClsDef = TypesUtil
                .createClassTypeDef(VIEW_TYPE, VIEW_TYPE, null,
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        new AttributeDefinition("db", DATABASE_TYPE, Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("inputTables", DataTypes.arrayTypeName(TABLE_TYPE),
                                Multiplicity.COLLECTION, false, null));

        ImmutableList<HierarchicalTypeDefinition<ClassType>> classes =
                ImmutableList.of(dbClsDef, storageDescClsDef, columnClsDef, tblClsDef, loadProcessClsDef, viewClsDef);

        return TypesUtil.getTypesDef(enums, structs, traits, classes);
    }

    /**
     *
     * @param navigatorNode
     * @return
     */
    private Referenceable createEntity(NavigatorNode navigatorNode) {
        //TODO Map given the navigatorType ID

        Referenceable dbRef = null;
        Id dbId = null;
        try {
            client.deleteEntity(DATABASE_TYPE, "name", "TESTDB");
            client.deleteEntity(TABLE_TYPE, "name", navigatorNode.getName());

            for(Map.Entry<String, String> entry : navigatorNode.getSchema().entrySet()) {
                client.deleteEntity(COLUMN_TYPE, "name", entry.getKey());
                client.deleteEntity(COLUMN_TYPE, "dataType", entry.getValue());
            }

            List<String> dbGuids = client.listEntities(DATABASE_TYPE);
            dbRef = client.getEntity(DATABASE_TYPE, "name", "TESTDB");
            dbId = dbRef.getId();
//            for (String dbGuid : dbGuids) {
//                Referenceable dbId = client.getEntity(dbGuid);
//                System.out.println(dbId);
//            }
//            Referenceable r = client.getEntity(DATABASE_TYPE, "", "");
//            System.out.println(dbs);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }

        if (dbId == null) {
            dbRef = new Referenceable(DATABASE_TYPE, "Dimension");
            dbRef.set("name", "TESTDB");
            dbRef.set("owner", "scott");
            dbRef.set("createTime", System.currentTimeMillis());
            dbId = createInstance(dbRef);
        }

        Referenceable sd = new Referenceable(STORAGE_DESC_TYPE);
        sd.set("location", "hdfs://host:8000/apps/warehouse/sales");
        sd.set("inputFormat", "TextInputFormat");
        sd.set("outputFormat", "TextOutputFormat");
        sd.set("compressed", true);

        Referenceable table = new Referenceable(TABLE_TYPE, "Dimension", "Fact");
        table.set("db", dbId);
        table.set("name", navigatorNode.getName());
        table.set("sd", sd);

        // columns comes from the father
        Map<String, String> schema = navigatorNode.getSchema();
        List<Referenceable> columns = new ArrayList<>();
        for(Map.Entry<String, String> entry : schema.entrySet()) {
            Referenceable column = new Referenceable(COLUMN_TYPE); //, "Metric"
            column.set("name", entry.getKey());
            column.set("dataType", entry.getValue());
            columns.add(column);
        }
        table.set("columns", columns);


        Referenceable process = new Referenceable(LOAD_PROCESS_TYPE, "ETL");
        // super type attributes
        process.set("name", navigatorNode.getName());
//        referenceable.set("description", description);
//        referenceable.set("inputs", ImmutableList.of(salesFact, timeDim));
//        referenceable.set("outputs", ImmutableList.of(salesFactDaily));
        process.set("inputs", ImmutableList.of());
        process.set("outputs", ImmutableList.of());

        process.set("user", "scott");
        process.set("startTime", System.currentTimeMillis());
        process.set("endTime", System.currentTimeMillis() + 10000);
        process.set("queryText", "queryText");
        process.set("queryPlan", "queryPlan");
        process.set("queryId", "queryId");
        process.set("queryGraph", "queryGraph");

//        return createInstance(referenceable);

//        // TODO These two are from IDs
//        List<String> inputNodes = navigatorNode.getInputNodes();
//        table.set("inputs", inputNodes);
//
//        List<String> outputNodes = navigatorNode.getOutputNodes();
//        table.set("outputs", outputNodes);

//        return table;
        return process;
    }

}
