package org.talend.governance.atlas;

import com.google.common.base.Functions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.*;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang3.tuple.Triple;
import org.talend.cloudera.navigator.api.NavigatorNode;

import java.util.*;


/**
 * Class to map the Talend DAG into an Atlas DAG
 */
public class AtlasMapper {

    static final String COLUMN_TYPE = "Column2";
    static final String TABLE_TYPE = "Table2";
    static final String LOAD_PROCESS_TYPE = "Process2";

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
        //TODO JB a method to create or get the ids of the typesDef
        TypesDef typesDef = createTypeDefinitions();
//        List<String> typesIds = persist(typesDef);
        // This is just for development reasons
//        Collection<String> types = AtlasUtils.getAllTypes();
//        System.out.println(types);

        // we create the Entities
//        Collection<Referenceable> refs = new ArrayList<>();
        try {
            AtlasUtils.deleteEntities(TABLE_TYPE, COLUMN_TYPE, LOAD_PROCESS_TYPE);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }

//        deleteEntitiesByType();

//        Collection<IInstance> refs = new ArrayList<>();
//        for (NavigatorNode navigatorNode : navigatorNodes) {
////            deleteEntity(navigatorNode);
//            Referenceable ref = createEntity(navigatorNode);
//            refs.add(ref);
//        }
//        AtlasUtils.createInstances(refs);

        Map<String, Triple<NavigatorNode, Referenceable, Id>> nodes = new HashMap<>();
        for (NavigatorNode navigatorNode : navigatorNodes) {
//            deleteEntity(navigatorNode);
            Referenceable ref = createEntity(navigatorNode);
//            ref.getId();
//            Id id = AtlasUtils.createInstance(ref);
            Id id = null;
            nodes.put(navigatorNode.getName(), Triple.of(navigatorNode, ref, id));
        }

        // Update links information afterwards
        for (Map.Entry<String, Triple<NavigatorNode, Referenceable, Id>> entry : nodes.entrySet()) {
            NavigatorNode navigatorNode = entry.getValue().getLeft();
            Referenceable ref = entry.getValue().getMiddle();
            String guid = entry.getValue().getRight()._getId();

            List<Id> inputIds = new ArrayList<>();
            for (String input : navigatorNode.getInputNodes()) {
                Triple<NavigatorNode, Referenceable, Id> t = nodes.get(input);
                inputIds.add(t.getRight());
            }
            ref.set("inputs", inputIds);

            Collection<Triple<NavigatorNode, Referenceable, Id>> in =
                    Collections2.transform(navigatorNode.getInputNodes(), Functions.forMap(nodes));

            List<Id> outputIds = new ArrayList<>();
            for (String output : navigatorNode.getOutputNodes()) {
                Triple<NavigatorNode, Referenceable, Id> t = nodes.get(output);
                inputIds.add(t.getRight());
            }
            ref.set("outputs", outputIds);

            try {
                Referenceable r2 = this.client.getEntity(guid);
                System.out.format("Updating %s: %s\n", r2.getId()._getId(), r2);
                System.out.println("---------------------------");
                System.out.format("Updating %s: %s\n", ref.getId()._getId(), ref);

                // guid ?
                this.client.updateEntity(r2.getId()._getId(), r2);

//                this.client.updateEntity(guid, ref);
//                this.client.updateEntityAttribute(id._getId(), "inputs", ids2);
            } catch (AtlasServiceException e) {
                e.printStackTrace();
            }

        }
        System.out.printf("DONE");


//        persist(refs);

        // verify types created
//        verifyTypesCreated();
    }

    public List<String> persist(TypesDef typesDef) {
//        System.out.println("typesAsJSON = " + TypesSerialization.toJson(typesDef));
        List<String> ids = new ArrayList<>();
        try {
            ids = this.client.createType(typesDef);
        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }
        return ids;
    }

//    private @Nullable Id createInstance(Referenceable referenceable) {
//        String entityJSON = InstanceSerialization.toJson(referenceable, true);
//        System.out.println("Submitting new entity= " + entityJSON);
//        JSONArray guids = null;
//        try {
//            guids = this.client.createEntity(entityJSON);
//            String typeName = referenceable.getTypeName();
//            System.out.println("created instance for type " + typeName + ", guid: " + guids);
//            // return the Id for created instance with guid
//            return new Id(guids.getString(guids.length()-1), referenceable.getId().getVersion(),
//                    referenceable.getTypeName());
////        } catch (AtlasServiceException e) {
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

//    public JSONArray persist(Collection<Referenceable> refs) {
//        for (Referenceable ref : refs) {
//            Id id = createInstance(ref);
//            System.out.println(InstanceSerialization.toJson(ref, true));
//        }
////
////        JSONArray jsonArray = new JSONArray();
////        try {
////            jsonArray = this.client.createEntity(refs);
////        } catch (AtlasServiceException e) {
////            e.printStackTrace();
////        }
////        return jsonArray;
//    }


    /**
     *
     * @return
     */
    private static TypesDef createTypeDefinitions() {
        ImmutableList<EnumTypeDefinition> enums = ImmutableList.<EnumTypeDefinition>of();
        ImmutableList<StructTypeDefinition> structs = ImmutableList.<StructTypeDefinition>of();

        //TODO tags as traits
        HierarchicalTypeDefinition<TraitType> dimTraitDef = TypesUtil.createTraitTypeDef("Dimension",  "Dimension Trait", null);
        HierarchicalTypeDefinition<TraitType> factTraitDef = TypesUtil.createTraitTypeDef("Fact", "Fact Trait", null);
        HierarchicalTypeDefinition<TraitType> piiTraitDef = TypesUtil.createTraitTypeDef("PII", "PII Trait", null);
        HierarchicalTypeDefinition<TraitType> metricTraitDef = TypesUtil.createTraitTypeDef("Metric", "Metric Trait", null);
        HierarchicalTypeDefinition<TraitType> etlTraitDef = TypesUtil.createTraitTypeDef("ETL", "ETL Trait", null);
        HierarchicalTypeDefinition<TraitType> jdbcTraitDef = TypesUtil.createTraitTypeDef("JdbcAccess", "JdbcAccess Trait", null);
        HierarchicalTypeDefinition<TraitType> logTraitDef = TypesUtil.createTraitTypeDef("Log Data", "LogData Trait",  null);

//        HierarchicalTypeDefinition<TraitType> talendTraitDef = TypesUtil.createTraitTypeDef("Talend",  "Talend Trait", null);
        ImmutableList<HierarchicalTypeDefinition<TraitType>> traits =
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of();
//                ImmutableList.of(dimTraitDef, factTraitDef, piiTraitDef, metricTraitDef,
//                        etlTraitDef, jdbcTraitDef, logTraitDef);

        HierarchicalTypeDefinition<ClassType> columnClsDef = TypesUtil
                .createClassTypeDef(COLUMN_TYPE, COLUMN_TYPE, null,
                        TypesUtil.createOptionalAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("type", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("comment", DataTypes.STRING_TYPE)
                );
        HierarchicalTypeDefinition<ClassType> tblClsDef = TypesUtil
                .createClassTypeDef(TABLE_TYPE, TABLE_TYPE, ImmutableSet.of("DataSet"),
                        TypesUtil.createOptionalAttrDef("owner", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("createTime", DataTypes.LONG_TYPE),
                        TypesUtil.createOptionalAttrDef("lastAccessTime", DataTypes.LONG_TYPE),
                        TypesUtil.createOptionalAttrDef("retention", DataTypes.LONG_TYPE),
                        TypesUtil.createOptionalAttrDef("viewOriginalText", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("viewExpandedText", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("tableType", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("temporary", DataTypes.BOOLEAN_TYPE),
                        new AttributeDefinition(COLUMNS_ATTRIBUTE, DataTypes.arrayTypeName(COLUMN_TYPE),
                                Multiplicity.COLLECTION, true, null)
                );
        HierarchicalTypeDefinition<ClassType> loadProcessClsDef = TypesUtil
                .createClassTypeDef(LOAD_PROCESS_TYPE, LOAD_PROCESS_TYPE, ImmutableSet.of("Process"),
                        TypesUtil.createOptionalAttrDef("user", DataTypes.STRING_TYPE),
                        TypesUtil.createOptionalAttrDef("startTime", DataTypes.LONG_TYPE),
                        TypesUtil.createOptionalAttrDef("endTime", DataTypes.LONG_TYPE),
                        TypesUtil.createOptionalAttrDef("query", DataTypes.STRING_TYPE),
                        new AttributeDefinition(COLUMNS_ATTRIBUTE, DataTypes.arrayTypeName(COLUMN_TYPE),
                                Multiplicity.COLLECTION, true, null)
                );
        ImmutableList<HierarchicalTypeDefinition<ClassType>> classes =
                ImmutableList.of(columnClsDef, tblClsDef, loadProcessClsDef);

        return TypesUtil.getTypesDef(enums, structs, traits, classes);
    }


    private void deleteEntity(NavigatorNode navigatorNode) {
//        try {
//            this.client.deleteEntities(cols);
//            this.client.deleteEntity(TABLE_TYPE, "name", navigatorNode.getName());
//            for(Map.Entry<String, String> entry : navigatorNode.getSchema().entrySet()) {
//                this.client.deleteEntity(COLUMN_TYPE, "name", entry.getKey());
//                this.client.deleteEntity(COLUMN_TYPE, "type", entry.getValue());
//            }
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
    }

    private Referenceable createEntity(NavigatorNode navigatorNode) {
//        Referenceable table = new Referenceable(TABLE_TYPE, "Dimension");
//        table.set("name", navigatorNode.getName());
////        table.set("description", navigatorNode.getName());
        Map<String, String> schema = navigatorNode.getSchema();
//        List<Referenceable> columns = new ArrayList<>();
//        for(Map.Entry<String, String> entry : schema.entrySet()) {
//            Referenceable column = new Referenceable(COLUMN_TYPE);
//            column.set("name", entry.getKey());
//            column.set("type", entry.getValue());
//            columns.add(column);
//        }
//        table.set("columns", columns);

        Referenceable process = new Referenceable(LOAD_PROCESS_TYPE, "Dimension", "ETL");
        process.set("name", navigatorNode.getName());
//        process.set("description", description);

        // TODO These two are from IDs
        process.set("inputs", ImmutableList.of());
        process.set("outputs", ImmutableList.of());

        process.set("user", "scott");
        process.set("startTime", System.currentTimeMillis());
        process.set("endTime", System.currentTimeMillis() + 10000);
        process.set("query", "queryText");
        List<Referenceable> columns = new ArrayList<>();
        for(Map.Entry<String, String> entry : schema.entrySet()) {
            Referenceable column = new Referenceable(COLUMN_TYPE); //"PII" or "Metric"
            column.set("name", entry.getKey());
            column.set("type", entry.getValue());
            columns.add(column);
        }
        process.set("columns", columns);

        Map<String, String> metadata = navigatorNode.getMetadata();
        for(Map.Entry<String, String> meta : metadata.entrySet()) {
            process.set(meta.getKey(), meta.getValue());
        }

//        client.updateEntityAttribute(guid, attribute, value);
//        return createInstance(referenceable);
//        return table;
        return process;
    }

}
