package org.talend.governance;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.IInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.IDataType;
import org.codehaus.jettison.json.JSONArray;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by ismael on 4/23/16.
 */
public class AtlasUtils {

    private static String ENDPOINT_URL = "http://localhost:21000";
    private static AtlasClient client =  new AtlasClient(ENDPOINT_URL);

    /**
     *
     * @param referenceable
     * @return
     * @throws AtlasException
     */
    public static Id createInstance(Referenceable referenceable) throws AtlasException {
        //TODO improve safety + exception handling
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
//            e.printStackTrace();
            throw new AtlasException(e);
        }
    }


    /**
     *
     * @param instan
     * @return
     */
    public static Object createInstances(Collection<Referenceable> refs) throws AtlasServiceException {
//    public static Object createInstances(Collection<? extends IInstance> instances) {
        //shouldn't the argument be Collection<? extends IInstance>
        //TODO JB a real (atomic) server side version, with a real return value for Id,
        // It must delete the entities if it cannot create them all
        // notice that current AtlasClient.createEntity(Collection...) does not guarantee this
        // Additionally it would be better to have the Ref objects and not the json as a return type
        client.createEntity(refs);
        // what should be the return type ? Map<Referenceable, Id> ? or otheeven better the Collection of
        // Referenceables with the IDs included ?
        return null;
    }

    /**
     * Deletes all the entities of the given types
     * @param types
     */
    public static void deleteEntities(String... types) throws AtlasServiceException {
        for (String type : types) {
            List<String> guids = client.listEntities(type);
            if (guids.size() > 0) {
                String[] guidAsArray = guids.toArray(new String[guids.size()]);
                client.deleteEntities(guidAsArray);
            }
        }
    }

//    public static void deleteAllEntities() {
//        try {
//            client.getEntity()
//        } catch (AtlasServiceException e) {
//            e.printStackTrace();
//        }
//    }

    /**
     *
     * @return the full list of types available
     */
    public static Collection<String> getAllTypes() {
        //This can be really big but for development/debug reasons it could be really useful
        // low priority too
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

    /**
     * Checks if the instance is a correct representation of the datatype,
     * this is useful to avoid calling the server to validate instances (refs)
     * @param dataType
     * @param ref
     * @throws AtlasException
     */
    public static void enforceTypes(IDataType dataType, IInstance instance) throws AtlasException {
        //TODO this one has lower priority but it would be a blessing to haves
    }
}
