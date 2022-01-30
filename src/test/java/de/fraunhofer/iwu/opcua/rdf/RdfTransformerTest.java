package de.fraunhofer.iwu.opcua.rdf;

import de.fraunhofer.iwu.opcua.util.OpcuaContext;
import de.fraunhofer.iwu.opcua.util.Transformer;
import org.eclipse.milo.examples.server.ExampleServer;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class RdfTransformerTest {

    static RdfTransformer transformer;
    static ExampleServer server;
    static IRI testEntryPoint;


    @BeforeAll
    static void setUp() throws Exception {
        testEntryPoint = new ValidatingValueFactory().createIRI("http://iwu.fraunhofer.de/c32/testEntryPoint");
        server = new ExampleServer();
        server.startup().get();
    }

    @AfterAll
    static void tearDown() {
        server.shutdown();
    }

    @Test
    void transform() {
        transformer = new RdfTransformer("opc.tcp://localhost:12686/milo", testEntryPoint);
        Model rdfModel = transformer.transform();
        transformer.save(rdfModel);
    }

    @Test
    @Disabled
    void transformWithManuallyStartedServer() {
        transformer = new RdfTransformer("opc.tcp://localhost:4840", testEntryPoint);
        Model rdfModel = transformer.transform();
        transformer.save(rdfModel);
    }
}