package org.talend.governance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Class to map the Talend DAG into an Atlas DAG
 */
public class AtlasMapper {

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
        Collection<Referenceable> refs = new ArrayList<Referenceable>();
        for (NavigatorNode navigatorNode : navigatorNodes) {
            Referenceable ref = createEntity(navigatorNode);
            refs.add(ref);
            String entityJSON = InstanceSerialization.toJson(ref, true);
            System.out.println(entityJSON);
        }

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

        HierarchicalTypeDefinition<TraitType> logTraitDef = TypesUtil.createTraitTypeDef("Log Data", null); //"LogData Trait",  null);
        ImmutableList<HierarchicalTypeDefinition<TraitType>> traits = ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(logTraitDef);

        ImmutableList<HierarchicalTypeDefinition<ClassType>> classes = ImmutableList.<HierarchicalTypeDefinition<ClassType>>of();

        return TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
            traits, classes);
    }

    /**
     *
     * @param navigatorNode
     * @return
     */
    private Referenceable createEntity(NavigatorNode navigatorNode) {
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
        return referenceable;
    }

}
