package de.fraunhofer.iwu.opcua.rdf;

import de.fraunhofer.iwu.opcua.util.OpcuaContext;
import de.fraunhofer.iwu.opcua.util.Transformer;
import org.eclipse.milo.examples.server.ExampleServer;
import org.eclipse.rdf4j.model.Model;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class RdfTransformerTest {

    static RdfTransformer transformer;
    static ExampleServer server;

    @BeforeAll
    static void setUp() throws Exception {
        server = new ExampleServer();
        server.startup().get();
    }

    @Test
    void transform() {
        transformer = new RdfTransformer("opc.tcp://localhost:12686/milo");
        Model rdfModel = transformer.transform();
        transformer.save(rdfModel);

    }

    @AfterAll
    static void tearDown() {
        server.shutdown();
    }
}