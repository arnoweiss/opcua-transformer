package de.fraunhofer.iwu.opcua.util;

import org.eclipse.milo.examples.server.ExampleServer;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class OpcuaContextTest {

    static OpcuaContext helper;
    static ExampleServer server;

    @BeforeAll
    public static void setUp() throws Exception {
        server = new ExampleServer();
        server.startup().get();
        helper = new OpcuaContext("opc.tcp://localhost:12686/milo");
    }

    @Test
    void browseAddressSpace() {
        Map<? extends UaNode, List<ReferenceDescription>> nodes = helper.getNodeRefMap();
        assertTrue(nodes.size() > 10);
        Set<String> nodeIds = nodes.keySet().stream().map(k -> k.getNodeId().toParseableString()).collect(Collectors.toSet());
        assertTrue(nodeIds.contains("ns=2;s=HelloWorld/Dynamic/Double"));
        UaNode dynamicBool = nodes.keySet().stream()
                .filter(k -> k.getNodeId().toParseableString().equals("ns=2;s=HelloWorld/Dynamic/Boolean"))
                .findFirst().orElseThrow(() -> new RuntimeException("Node not found in nodes"));
        assertEquals(1, nodes.get(dynamicBool).size());
    }

    @Test
    void getEndpointDescriptions() {
        List<EndpointDescription> endpointDescriptions = helper.getEndpointDescriptions();
        assertTrue(endpointDescriptions.size() > 0);
    }

    @Test
    void getClient() {
        OpcUaClient c = helper.getClient();
        assertTrue(Arrays.stream(c.getNamespaceTable().toArray()).anyMatch("http://opcfoundation.org/UA/"::equals));
    }

    @AfterAll
    public static void tearDown() throws ExecutionException, InterruptedException {
        helper.getClient().disconnect();
        server.shutdown().get();

    }
}