package de.fraunhofer.iwu.opcua.rdf;

import de.fraunhofer.iwu.opcua.util.OpcuaContext;
import de.fraunhofer.iwu.opcua.util.Transformer;
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
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.OuterQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPattern;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RdfTransformerTest {

    static RdfTransformer transformer;
    static ExampleServer server;
    static IRI testEntryPoint;
    static ValueFactory v;


    @BeforeAll
    static void setUp() throws Exception {
        v = new ValidatingValueFactory();
        testEntryPoint = v.createIRI("http://iwu.fraunhofer.de/c32/testEntryPoint");
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
        assertTrue(Stream.of(rdfModel.getStatements(null, null, null)).count() > Long.parseLong("100"));
        transformer.save(rdfModel);
    }

    @Test
    void attachAndSparql() {
        Repository db = new SailRepository(new MemoryStore());
        Model dummy = buildDummyGraph();
        IRI entry = v.createIRI("http://iwu.fraunhofer.de/c32/Machine/1");

        transformer = new RdfTransformer("opc.tcp://localhost:12686/milo", entry);
        Model rdfModel = transformer.transform();
        dummy.getStatements(null, null, null).forEach(rdfModel::add);
        try (RepositoryConnection conn = db.getConnection()) {
            rdfModel.getStatements(null, null, null).forEach(conn::add);
            TupleQuery tquery = conn.prepareTupleQuery(getTestQuery().getQueryString());
            try (TupleQueryResult result = tquery.evaluate()) {
                result.stream().forEach(bn -> System.out.println(bn.getValue("machines")));
            }
        }
    }

    @Test
    @Disabled
    void transformWithManuallyStartedServer() {
        transformer = new RdfTransformer("opc.tcp://localhost:4840", testEntryPoint);
        Model rdfModel = transformer.transform();
        transformer.save(rdfModel);
    }

    OuterQuery<?> getTestQuery() {
        SelectQuery selectQuery = Queries.SELECT();
        Variable machines = SparqlBuilder.var("machines");
        Variable roots = SparqlBuilder.var("rootNodes");
        TriplePattern allMachines = GraphPatterns.tp(machines, RDF.TYPE, v.createIRI("http://iwu.fraunhofer.de/c32/Machine"));
        TriplePattern withOpcuaAddrSpace = GraphPatterns.tp(machines, v.createIRI("http://iwu.fraunhofer.de/c32/hasOpcuaAddressSpace"), roots);
        SelectQuery allMachinesWithUaServers = selectQuery.select(machines).where(allMachines.and(withOpcuaAddrSpace));
        assertFalse(allMachinesWithUaServers.getQueryString().isEmpty());
        return allMachinesWithUaServers;
    }

    @Test
    void printTestQuery() {
        System.out.println(getTestQuery().getQueryString());
    }

    Model buildDummyGraph() {
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

    Model buildDummyAAS() {
        ModelBuilder b = new ModelBuilder();

        return b.build();
    }

}