package de.fraunhofer.iwu.opcua.util;

import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TransformerUtilsTest {

    @Test
    void getUriFromNodeId() {
        Map<NodeId, String> nodeIdsMap = new HashMap<>();
        NamespaceTable nst = new NamespaceTable();
        nst.addUri("http://opcfoundation.org/UA/");
        nst.addUri("http://opcfoundation.org/UA/DI/");
        nst.addUri("http://opcfoundation.org/UA/Machinery");

        nodeIdsMap.put(NodeId.parse("i=5"), "http://opcfoundation.org/UA/5");
        nodeIdsMap.put(NodeId.parse("ns=1;i=15031"), "http://opcfoundation.org/UA/DI/15031");
        nodeIdsMap.put(NodeId.parse("ns=2;i=6001"), "http://opcfoundation.org/UA/Machinery/6001");

        nodeIdsMap.entrySet().forEach(e -> TransformerUtils.getUriFromNodeId(e.getKey(), nst).equals(e.getValue()));
    }

    @Test
    void getLastUriSegment() {
        List<String> uris = new ArrayList<String>();
        uris.add("http://opcfoundation.org/UA/DI");
        uris.add("http://opcfoundation.org/UA/DI/");
        uris.add("http://opcfoundation.org/UA/DI#Rand");
        uris.forEach(uri -> {
            assertEquals("DI", TransformerUtils.getLastUriSegment(uri));
        });
    }
}