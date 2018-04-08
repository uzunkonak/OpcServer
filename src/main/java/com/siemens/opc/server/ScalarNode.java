package com.siemens.opc.server;

import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;

public class ScalarNode {
    private final String nodeName;
    private final NodeId nodeId;
    private final Variant variant;

    public ScalarNode(String nodeName, NodeId nodeId, Variant variant) {
        this.nodeName = nodeName;
        this.nodeId = nodeId;
        this.variant = variant;
    }

    public String getNodeName() {
        return nodeName;
    }

    public NodeId getNodeId() {
        return nodeId;
    }

    public Variant getVariant() {
        return variant;
    }
}
