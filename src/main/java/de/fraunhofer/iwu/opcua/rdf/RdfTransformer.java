package de.fraunhofer.iwu.opcua.rdf;

import de.fraunhofer.iwu.opcua.util.OpcuaContext;
import de.fraunhofer.iwu.opcua.util.Transformer;
import de.fraunhofer.iwu.opcua.util.TransformerUtils;
import org.eclipse.milo.opcua.sdk.client.nodes.*;
import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Statements;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RdfTransformer implements Transformer<Model> {

    OpcuaContext ctx;

    public RdfTransformer(String endpointUrl) {
        ctx = new OpcuaContext(endpointUrl);
    }

    @Override
    public Model transform() {
        ModelBuilder builder = new ModelBuilder();
        Model model = builder.build();
        NamespaceTable namespaces = ctx.getNamespaces();
        Arrays.stream(namespaces.toArray()).forEach(ns -> {
            model.setNamespace(TransformerUtils.getLastUriSegment(ns), ns);
        });

        Map<UaObjectNode, List<ReferenceDescription>> objects = ctx.getNodeRefMap().entrySet().stream()
                .filter(k -> k.getKey().getNodeClass().equals(NodeClass.Object))
                .collect(Collectors.toMap(e -> (UaObjectNode) e.getKey(), e -> e.getValue()));
        model.addAll(transformObjectNodes(objects));

        Map<UaObjectTypeNode, List<ReferenceDescription>> objectTypes = ctx.getNodeRefMap().entrySet().stream()
                .filter(k -> k.getKey().getNodeClass().equals(NodeClass.ObjectType))
                .collect(Collectors.toMap(e -> (UaObjectTypeNode) e.getKey(), e -> e.getValue()));
        model.addAll(transformObjectTypeNodes(objectTypes));

        Map<UaVariableNode, List<ReferenceDescription>> variables = ctx.getNodeRefMap().entrySet().stream()
                .filter(k -> k.getKey().getNodeClass().equals(NodeClass.Variable))
                .collect(Collectors.toMap(e -> (UaVariableNode) e.getKey(), e -> e.getValue()));
        model.addAll(transformVariableNodes(variables));

        Map<UaVariableTypeNode, List<ReferenceDescription>> variableTypes = ctx.getNodeRefMap().entrySet().stream()
                .filter(k -> k.getKey().getNodeClass().equals(NodeClass.VariableType))
                .collect(Collectors.toMap(e -> (UaVariableTypeNode) e.getKey(), e -> e.getValue()));
        model.addAll(transformVariableTypeNodes(variableTypes));

        Map<UaMethodNode, List<ReferenceDescription>> methods = ctx.getNodeRefMap().entrySet().stream()
                .filter(k -> k.getKey().getNodeClass().equals(NodeClass.Method))
                .collect(Collectors.toMap(e -> (UaMethodNode) e.getKey(), e -> e.getValue()));
        model.addAll(transformMethodNodes(methods));

        Map<UaReferenceTypeNode, List<ReferenceDescription>> referenceTypes = ctx.getNodeRefMap().entrySet().stream()
                .filter(k -> k.getKey().getNodeClass().equals(NodeClass.ReferenceType))
                .collect(Collectors.toMap(e -> (UaReferenceTypeNode) e.getKey(), e -> e.getValue()));
        model.addAll(transformReferenceTypeNodes(referenceTypes));

        Map<UaViewNode, List<ReferenceDescription>> views = ctx.getNodeRefMap().entrySet().stream()
                .filter(k -> k.getKey().getNodeClass().equals(NodeClass.View))
                .collect(Collectors.toMap(e -> (UaViewNode) e.getKey(), e -> e.getValue()));
        model.addAll(transformViewNodes(views));

        Map<UaDataTypeNode, List<ReferenceDescription>> dataTypes = ctx.getNodeRefMap().entrySet().stream()
                .filter(k -> k.getKey().getNodeClass().equals(NodeClass.DataType))
                .collect(Collectors.toMap(e -> (UaDataTypeNode) e.getKey(), e -> e.getValue()));
        model.addAll(transformDataTypeNodes(dataTypes));

        return model;
    }

    private Model transformEndpointsAndAttach(Model m) {
        return m;

    }

    private List<Statement> transformObjectNodes(Map<UaObjectNode, List<ReferenceDescription>> objectNodes) {
        ArrayList<Statement> s =  new ArrayList<>();
        Function<Map.Entry<UaObjectNode, List<ReferenceDescription>>, List<Statement>> pred = e -> {
            ArrayList<Statement> l =  new ArrayList<>();
            l.addAll(transformGenericNode(e.getKey()));
            return l;
        };
        objectNodes.entrySet().stream().map(pred).collect(Collectors.toList());
        return s;
    }

    private List<Statement> transformObjectTypeNodes(Map<UaObjectTypeNode, List<ReferenceDescription>> objectTypeNodes) {
        return new ArrayList<Statement>();
    }

    private List<Statement> transformVariableNodes(Map<UaVariableNode, List<ReferenceDescription>> variableNodes) {
        return new ArrayList<Statement>();
    }

    private List<Statement> transformVariableTypeNodes(Map<UaVariableTypeNode, List<ReferenceDescription>> variableTypeNodes) {
        return new ArrayList<Statement>();
    }

    private List<Statement> transformMethodNodes(Map<UaMethodNode, List<ReferenceDescription>> methodNodes) {
        return new ArrayList<Statement>();
    }

    private List<Statement> transformDataTypeNodes(Map<UaDataTypeNode, List<ReferenceDescription>> dataTypeNodes) {
        return new ArrayList<Statement>();
    }

    private List<Statement> transformReferenceTypeNodes(Map<UaReferenceTypeNode, List<ReferenceDescription>> referenceTypeNodes) {
        return new ArrayList<Statement>();
    }

    private List<Statement> transformViewNodes(Map<UaViewNode, List<ReferenceDescription>> viewNodes) {
        return new ArrayList<Statement>();
    }

    private List<Statement> transformGenericNode(UaNode node) {
        ArrayList<Statement> l = new ArrayList<>();
        return l;
    }

}
