package org.talend.governance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.*;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONArray;
import org.talend.cloudera.navigator.api.NavigatorNode;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by ismael on 4/15/16.
 */
public class AtlasMapper {

    public void map(List<NavigatorNode> navigatorNodes, String jobId) {
        // First create the type definition
        TypesDef typesDef = createTypeDefinitions();
        String typesAsJSON = TypesSerialization.toJson(typesDef);
        System.out.println("typesAsJSON = " + typesAsJSON);

        // then we create the Entities
        Collection<Referenceable> refs = new ArrayList<Referenceable>();
        for (NavigatorNode navigatorNode : navigatorNodes) {
            String name = navigatorNode.getName();
            Map<String, String> schema = navigatorNode.getSchema();
            List<String> inputNodes = navigatorNode.getInputNodes();
            List<String> outputNodes = navigatorNode.getOutputNodes();

            Referenceable referenceable = new Referenceable(name, "TODO");
            for(Map.Entry<String, String> schem : schema.entrySet()) {
                referenceable.set(schem.getKey(), schem.getValue());
            }
            referenceable.set("inputs", inputNodes);
            referenceable.set("outputs", outputNodes);

            refs.add(referenceable);
            String entityJSON = InstanceSerialization.toJson(referenceable, true);
            System.out.println(entityJSON);
        }
//        AtlasClient client = null;
//        JSONArray guids = client.createEntity(refs);

//        metadataServiceClient.createType(typesAsJSON);
//        // verify types created
//        verifyTypesCreated();
    }

    TypesDef createTypeDefinitions() {
        List<EnumTypeDefinition> enums = ImmutableList.<EnumTypeDefinition>of();
        List<StructTypeDefinition> structs = ImmutableList.<StructTypeDefinition>of();

        HierarchicalTypeDefinition<TraitType> logTraitDef = TypesUtil.createTraitTypeDef("Log Data", null); //"LogData Trait",  null);
        List<HierarchicalTypeDefinition<TraitType>> traits = ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(logTraitDef);

        List<HierarchicalTypeDefinition<ClassType>> classes = ImmutableList.<HierarchicalTypeDefinition<ClassType>>of();

//        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
//                ImmutableList.of(), ImmutableList.of());
        return new TypesDef(JavaConversions.asScalaBuffer(enums), JavaConversions.asScalaBuffer(structs),
                    JavaConversions.asScalaBuffer(traits), JavaConversions.asScalaBuffer(classes));
    }

//    TypesDef createTypeDefinitions() throws Exception {
//        HierarchicalTypeDefinition<ClassType> dbClsDef = TypesUtil
//                .createClassTypeDef(DATABASE_TYPE, DATABASE_TYPE, null,
//                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
//                        attrDef("description", DataTypes.STRING_TYPE), attrDef("locationUri", DataTypes.STRING_TYPE),
//                        attrDef("owner", DataTypes.STRING_TYPE), attrDef("createTime", DataTypes.LONG_TYPE));
//
//        HierarchicalTypeDefinition<ClassType> storageDescClsDef = TypesUtil
//                .createClassTypeDef(STORAGE_DESC_TYPE, STORAGE_DESC_TYPE, null, attrDef("location", DataTypes.STRING_TYPE),
//                        attrDef("inputFormat", DataTypes.STRING_TYPE), attrDef("outputFormat", DataTypes.STRING_TYPE),
//                        attrDef("compressed", DataTypes.STRING_TYPE, Multiplicity.REQUIRED, false, null));
//
//        HierarchicalTypeDefinition<ClassType> columnClsDef = TypesUtil
//                .createClassTypeDef(COLUMN_TYPE, COLUMN_TYPE, null, attrDef("name", DataTypes.STRING_TYPE),
//                        attrDef("dataType", DataTypes.STRING_TYPE), attrDef("comment", DataTypes.STRING_TYPE));
//
//        HierarchicalTypeDefinition<ClassType> tblClsDef = TypesUtil
//                .createClassTypeDef(TABLE_TYPE, TABLE_TYPE, ImmutableSet.of("DataSet"),
//                        new AttributeDefinition(DB_ATTRIBUTE, DATABASE_TYPE, Multiplicity.REQUIRED, false, null),
//                        new AttributeDefinition("sd", STORAGE_DESC_TYPE, Multiplicity.REQUIRED, true, null),
//                        attrDef("owner", DataTypes.STRING_TYPE), attrDef("createTime", DataTypes.LONG_TYPE),
//                        attrDef("lastAccessTime", DataTypes.LONG_TYPE), attrDef("retention", DataTypes.LONG_TYPE),
//                        attrDef("viewOriginalText", DataTypes.STRING_TYPE),
//                        attrDef("viewExpandedText", DataTypes.STRING_TYPE), attrDef("tableType", DataTypes.STRING_TYPE),
//                        attrDef("temporary", DataTypes.BOOLEAN_TYPE),
//                        new AttributeDefinition(COLUMNS_ATTRIBUTE, DataTypes.arrayTypeName(COLUMN_TYPE),
//                                Multiplicity.COLLECTION, true, null));
//
//        HierarchicalTypeDefinition<ClassType> loadProcessClsDef = TypesUtil
//                .createClassTypeDef(LOAD_PROCESS_TYPE, LOAD_PROCESS_TYPE, ImmutableSet.of("Process"),
//                        attrDef("userName", DataTypes.STRING_TYPE), attrDef("startTime", DataTypes.LONG_TYPE),
//                        attrDef("endTime", DataTypes.LONG_TYPE),
//                        attrDef("queryText", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
//                        attrDef("queryPlan", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
//                        attrDef("queryId", DataTypes.STRING_TYPE, Multiplicity.REQUIRED),
//                        attrDef("queryGraph", DataTypes.STRING_TYPE, Multiplicity.REQUIRED));
//
//        HierarchicalTypeDefinition<ClassType> viewClsDef = TypesUtil
//                .createClassTypeDef(VIEW_TYPE, VIEW_TYPE, null,
//                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
//                        new AttributeDefinition("db", DATABASE_TYPE, Multiplicity.REQUIRED, false, null),
//                        new AttributeDefinition("inputTables", DataTypes.arrayTypeName(TABLE_TYPE),
//                                Multiplicity.COLLECTION, false, null));
//
//        HierarchicalTypeDefinition<TraitType> dimTraitDef = TypesUtil.createTraitTypeDef("Dimension",  "Dimension Trait", null);
//
//        HierarchicalTypeDefinition<TraitType> factTraitDef = TypesUtil.createTraitTypeDef("Fact", "Fact Trait", null);
//
//        HierarchicalTypeDefinition<TraitType> piiTraitDef = TypesUtil.createTraitTypeDef("PII", "PII Trait", null);
//
//        HierarchicalTypeDefinition<TraitType> metricTraitDef = TypesUtil.createTraitTypeDef("Metric", "Metric Trait", null);
//
//        HierarchicalTypeDefinition<TraitType> etlTraitDef = TypesUtil.createTraitTypeDef("ETL", "ETL Trait", null);
//
//        HierarchicalTypeDefinition<TraitType> jdbcTraitDef = TypesUtil.createTraitTypeDef("JdbcAccess", "JdbcAccess Trait", null);
//
//        HierarchicalTypeDefinition<TraitType> logTraitDef = TypesUtil.createTraitTypeDef("Log Data", "LogData Trait",  null);
//
//        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
//                ImmutableList.of(dimTraitDef, factTraitDef, piiTraitDef, metricTraitDef, etlTraitDef, jdbcTraitDef, logTraitDef),
//                ImmutableList.of(dbClsDef, storageDescClsDef, columnClsDef, tblClsDef, loadProcessClsDef, viewClsDef));
//    }


    public static void main(String[] args) {

    }
}
