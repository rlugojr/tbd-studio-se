package org.talend.governance;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
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
    static final String TABLE_TYPE = "Table";
    static final String COLUMN_TYPE = "Column";
    static final String STORAGE_DESC_TYPE = "StorageDesc";

    public static final String DB_ATTRIBUTE = "db";
    public static final String COLUMNS_ATTRIBUTE = "columns";

    /**
     * TODO this function must return the appropriate type of the objects that are going to be persisted
     * @param navigatorNodes
     * @param jobId
     */
    public void map(List<NavigatorNode> navigatorNodes, String jobId) {
        // First create the type definition
        TypesDef typesDef = createTypeDefinitions();
        String typesAsJSON = TypesSerialization.toJson(typesDef);
        System.out.println("typesAsJSON = " + typesAsJSON);

        // then we create the Entities
        Collection<Referenceable> refs = new ArrayList<>();
        for (NavigatorNode navigatorNode : navigatorNodes) {
            Referenceable ref = createEntity(navigatorNode);
            refs.add(ref);
            String entityJSON = InstanceSerialization.toJson(ref, true);
            System.out.println(entityJSON);
        }

//        Collection<Referenceable> refs2 = navigatorNodes.stream()
//                .map(node -> createEntity(node))
//                .collect(Collectors.toList());
//        refs2.stream()
//                .map(ref -> InstanceSerialization.toJson(ref, true))
//                .forEach(System.out::println);

//        AtlasClient client = null;
//        try {
//            JSONArray guids = client.createEntity(refs);
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }

//        metadataServiceClient.createType(typesAsJSON);
//        // verify types created
//        verifyTypesCreated();
    }

    /**
     *
     * @return
     */
    private TypesDef createTypeDefinitions() {
        ImmutableList<EnumTypeDefinition> enums = ImmutableList.<EnumTypeDefinition>of();
        ImmutableList<StructTypeDefinition> structs = ImmutableList.<StructTypeDefinition>of();

        ImmutableList<HierarchicalTypeDefinition<TraitType>> traits = ImmutableList.<HierarchicalTypeDefinition<TraitType>>of();

        HierarchicalTypeDefinition<ClassType> dbClsDef = TypesUtil
                .createClassTypeDef(DATABASE_TYPE, DATABASE_TYPE, null,
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        attrDef("description", DataTypes.STRING_TYPE),
                        attrDef("locationUri", DataTypes.STRING_TYPE),
                        attrDef("owner", DataTypes.STRING_TYPE),
                        attrDef("createTime", DataTypes.LONG_TYPE));

        HierarchicalTypeDefinition<ClassType> tblClsDef = TypesUtil
                .createClassTypeDef(TABLE_TYPE, TABLE_TYPE, ImmutableSet.of("DataSet"),
                        new AttributeDefinition(DB_ATTRIBUTE, DATABASE_TYPE, Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition("sd", STORAGE_DESC_TYPE, Multiplicity.REQUIRED, true, null),
                        attrDef("owner", DataTypes.STRING_TYPE),
                        attrDef("createTime", DataTypes.LONG_TYPE),
                        attrDef("lastAccessTime", DataTypes.LONG_TYPE),
                        attrDef("retention", DataTypes.LONG_TYPE),
                        attrDef("viewOriginalText", DataTypes.STRING_TYPE),
                        attrDef("viewExpandedText", DataTypes.STRING_TYPE),
                        attrDef("tableType", DataTypes.STRING_TYPE),
                        attrDef("temporary", DataTypes.BOOLEAN_TYPE),
                        new AttributeDefinition(COLUMNS_ATTRIBUTE, DataTypes.arrayTypeName(COLUMN_TYPE),
                                Multiplicity.COLLECTION, true, null));

        HierarchicalTypeDefinition<ClassType> columnClsDef = TypesUtil
                .createClassTypeDef(COLUMN_TYPE, COLUMN_TYPE, null,
                        attrDef("name", DataTypes.STRING_TYPE),
                        attrDef("dataType", DataTypes.STRING_TYPE),
                        attrDef("comment", DataTypes.STRING_TYPE));

        ImmutableList<HierarchicalTypeDefinition<ClassType>> classes =
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of(tblClsDef, columnClsDef);

        return TypesUtil.getTypesDef(enums, structs, traits, classes);
    }


    AttributeDefinition attrDef(String name, IDataType dT) {
        return attrDef(name, dT, Multiplicity.OPTIONAL, false, null);
    }

    AttributeDefinition attrDef(String name, IDataType dT, Multiplicity m, boolean isComposite,
                                String reverseAttributeName) {
        return new AttributeDefinition(name, dT.getName(), m, isComposite, reverseAttributeName);
    }

    /**
     *
     * @param navigatorNode
     * @return
     */
    private Referenceable createEntity(NavigatorNode navigatorNode) {
        Referenceable table = new Referenceable(TABLE_TYPE);
        table.set("name", navigatorNode.getName());

        Map<String, String> schema = navigatorNode.getSchema();
        List<Referenceable> columns = new ArrayList<>();
        for(Map.Entry<String, String> entry : schema.entrySet()) {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("name", entry.getKey());
            metadata.put("dataType", entry.getValue());
            columns.add(new Referenceable(COLUMN_TYPE, metadata));
        }

//        List<Referenceable> columns = schema.entrySet().stream()
//                .map(entry -> {
//                    Map<String, Object> m = Maps.newHashMap();
//                    m.put("name", entry.getKey());
//                    m.put("dataType", entry.getValue());
//                    return new Referenceable(COLUMN_TYPE, m);
//                })
//                .collect(Collectors.<Referenceable>toList());

//        List<Referenceable> columns = schema.entrySet().stream()
//                .map(entry -> {
//                    return new Referenceable(COLUMN_TYPE,
//                            new ImmutableMap.Builder<String, Object>()
//                                    .put("name", entry.getKey())
//                                    .put("dataType", entry.getValue()).build());})
//                .collect(Collectors.<Referenceable>toList());

        table.set("columns", columns);

        // TODO These two are from IDs
        List<String> inputNodes = navigatorNode.getInputNodes();
        table.set("inputs", inputNodes);

        List<String> outputNodes = navigatorNode.getOutputNodes();
        table.set("outputs", outputNodes);

        return table;
    }

}
