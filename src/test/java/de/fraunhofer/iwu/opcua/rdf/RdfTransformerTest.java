package de.fraunhofer.iwu.opcua.rdf;

import de.fraunhofer.iwu.opcua.util.OpcuaContext;
import org.eclipse.milo.examples.server.ExampleServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RdfTransformerTest {

    OpcuaContext ctx;
    RdfTransformer transformer;
    ExampleServer server;

    public RdfTransformerTest() throws Exception {
        server = new ExampleServer();
        server.startup().get();
        ctx = new OpcuaContext("opc.tcp://localhost:12686/milo");
    }

    @BeforeEach
    void setUp() {

    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void transform() {
    }
}