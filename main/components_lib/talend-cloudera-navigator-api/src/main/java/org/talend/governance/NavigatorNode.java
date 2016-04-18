package org.talend.governance;

import java.util.List;
import java.util.Map;

public class NavigatorNode extends org.talend.cloudera.navigator.api.NavigatorNode {
    public NavigatorNode(String name, Map<String, String> schema, List<String> inputNodes, List<String> outputNodes) {
        super(name, schema, inputNodes, outputNodes);
    }
}
