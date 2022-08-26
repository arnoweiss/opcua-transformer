package de.fraunhofer.iwu.opcua.rdf;

import org.eclipse.milo.examples.server.ExampleServer;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sparqlbuilder.core.QueryElement;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.OuterQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RdfTransformerTest {

    static RdfTransformer transformer;
    static ExampleServer server;
    static IRI adaptionPoint;
    static ValueFactory v;


    @BeforeAll
    static void setUp() throws Exception {
        v = new ValidatingValueFactory();
        adaptionPoint = v.createIRI("http://iwu.fraunhofer.de/c32/testAdaptionPoint");
        server = new ExampleServer();
        server.startup().get();
    }

    @AfterAll
    static void tearDown() {
        server.shutdown();
    }

    @Test
    void transform() {
        transformer = new RdfTransformer("opc.tcp://localhost:12686/milo", adaptionPoint);
        Model rdfModel = transformer.transform();
        assertTrue(rdfModel.size()> 100);
        transformer.save(rdfModel);
    }

    @Test
    void expAttachAndSparql() {
        Repository db = new SailRepository(new MemoryStore());
        Model dummy = buildDummyGraph();
        IRI adaptPoint = v.createIRI("http://iwu.fraunhofer.de/c32/Machine/1");

        transformer = new RdfTransformer("opc.tcp://localhost:12686/milo", adaptPoint);
        Model rdfModel = transformer.transform();
        dummy.getStatements(null, null, null).forEach(rdfModel::add);
        try (RepositoryConnection conn = db.getConnection()) {
            rdfModel.getStatements(null, null, null).forEach(conn::add);
            TupleQuery tquery = conn.prepareTupleQuery(getExpTestQuery().getQueryString());
            try (TupleQueryResult result = tquery.evaluate()) {
                result.stream().forEach(bn -> System.out.println(bn.getValue("machines")));
                assertFalse(result.getBindingNames().isEmpty());
//                assertEquals("http://iwu.fraunhofer.de/c32/Machine/1", result.getBindingNames().get(0));
            }
        }
    }

    @Test
    void aasAttachAndSparql() throws IOException {
        Model aas = buildDummyAAS();
        Repository db = new SailRepository(new MemoryStore());
        IRI adaptPoint = v.createIRI("http://customer.com/aas/9175_7013_7091_9168");
        transformer = new RdfTransformer("opc.tcp://localhost:12686/milo", adaptPoint);
        Model rdfModel = transformer.transform();
        aas.getStatements(null, null, null).forEach(rdfModel::add);
        try (RepositoryConnection conn = db.getConnection()) {
            rdfModel.getStatements(null, null, null).forEach(conn::add);
            TupleQuery tquery = conn.prepareTupleQuery(getAasTestQuery().getQueryString());
            try (TupleQueryResult result = tquery.evaluate()) {
                assertEquals("http://customer.com/aas/9175_7013_7091_9168", result.stream().findFirst().orElseThrow().getValue("aas").stringValue());
            }
        }
    }


    @Test
    @Disabled
    void transformWithManuallyStartedServer() {
        transformer = new RdfTransformer("opc.tcp://localhost:4840", adaptionPoint);
        Model rdfModel = transformer.transform();
        transformer.save(rdfModel);
    }

    private QueryElement getAasTestQuery() {
        SelectQuery selectQuery = Queries.SELECT();
        Variable aas = SparqlBuilder.var("aas");
        Variable roots = SparqlBuilder.var("rootNodes");
        TriplePattern allAas = GraphPatterns.tp(aas, RDF.TYPE, v.createIRI("https://admin-shell.io/aas/3/0/RC01/AssetAdministrationShell"));
        TriplePattern withOpcuaAddrSpace = GraphPatterns.tp(aas, v.createIRI("http://iwu.fraunhofer.de/c32/hasOpcuaAddressSpace"), roots);
        SelectQuery allAasWithUaServers = selectQuery.select(aas).where(allAas.and(withOpcuaAddrSpace));
        assertFalse(allAasWithUaServers.getQueryString().isEmpty());
        return allAasWithUaServers;
    }


    private OuterQuery<?> getExpTestQuery() {
        SelectQuery selectQuery = Queries.SELECT();
        Variable machines = SparqlBuilder.var("machines");
        Variable roots = SparqlBuilder.var("rootNodes");
        TriplePattern allMachines = GraphPatterns.tp(machines, RDF.TYPE, v.createIRI("http://iwu.fraunhofer.de/c32/Machine"));
        TriplePattern withOpcuaAddrSpace = GraphPatterns.tp(machines, v.createIRI("http://iwu.fraunhofer.de/c32/hasOpcuaAddressSpace"), roots);
        SelectQuery allMachinesWithUaServers = selectQuery.select(machines).where(allMachines.and(withOpcuaAddrSpace));
        assertFalse(allMachinesWithUaServers.getQueryString().isEmpty());
        return allMachinesWithUaServers;
    }

    private Model buildDummyGraph() {
        ModelBuilder b = new ModelBuilder();
        b.add(v.createIRI("http://iwu.fraunhofer.de/c32/Machine"), RDF.TYPE, RDFS.CLASS);
        Arrays.stream(new int[]{1, 2, 3, 4, 5}).forEach(num -> {
            IRI subject = v.createIRI("http://iwu.fraunhofer.de/c32/Machine/" + num);
            b.add(subject, RDFS.LABEL, v.createLiteral("Machine" + num));
            b.add(subject, v.createIRI("http://iwu.fraunhofer.de/c32/hasCapability"), v.createIRI("http://iwu.fraunhofer.de/c32/capa/" + subject.hashCode()));
            b.add(subject, RDF.TYPE, v.createIRI("http://iwu.fraunhofer.de/c32/Machine"));
        });
        return b.build();
    }

    private Model buildDummyAAS() throws IOException {

        File file = new File("src/test/resources/AssetAdministrationShell_Example.ttl");
        FileInputStream fis = new FileInputStream(file);
        Model aas = Rio.parse(fis, RDFFormat.TURTLE);

        return aas;
    }

}