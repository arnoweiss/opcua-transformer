package de.fraunhofer.iwu.opcua.rdf;

import de.fraunhofer.iwu.opcua.util.OpcuaContext;
import de.fraunhofer.iwu.opcua.util.Transformer;
import org.eclipse.milo.opcua.sdk.client.DataTypeTreeSessionInitializer;
import org.eclipse.milo.opcua.sdk.client.nodes.*;
import org.eclipse.milo.opcua.sdk.core.DataTypeTree;
import org.eclipse.milo.opcua.stack.core.BuiltinDataType;
import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UNumber;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeAttributesMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RdfTransformer implements Transformer<Model> {

    OpcuaContext ctx;
    IRI currentSubject;
    IRI entryPoint;
    Logger logger;
    ValueFactory v;
    DataTypeTree dataTypeTree;


    public RdfTransformer(String endpointUrl, IRI entry) {
        ctx = new OpcuaContext(endpointUrl);
        logger = LoggerFactory.getLogger(RdfTransformer.class);
        v = new ValidatingValueFactory();
        try {
            dataTypeTree = (DataTypeTree) ctx.getClient().getSession().get().getAttribute(DataTypeTreeSessionInitializer.SESSION_ATTRIBUTE_KEY);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        entryPoint = entry;

    }

    @Override
    public void save(Model model) {
        String now = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME);
        String lastNamespace = ctx.getNamespaces().getUri(ctx.getNamespaces().toArray().length - 1);
        OutputStream output = OutputStream.nullOutputStream();
        File file = new File("target/output/transformed/" + lastNamespace + now + ".ttl");
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        try (OutputStream os = new FileOutputStream(file, false)) {
            Rio.write(model, os, RDFFormat.TURTLE);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Model transform() {
        ModelBuilder builder = new ModelBuilder();
        NamespaceTable namespaces = ctx.getNamespaces();
        Arrays.stream(namespaces.toArray()).forEach(ns -> {
            builder.setNamespace(TransformerUtils.getLastIdentifierSegment(ns), ns);
        });

        Map<UaObjectNode, List<ReferenceDescription>> objects = ctx.getNodeRefMap().entrySet().stream().filter(k -> k.getKey().getNodeClass().equals(NodeClass.Object)).collect(Collectors.toMap(e -> (UaObjectNode) e.getKey(), e -> e.getValue()));
        transformObjectNodes(objects).forEach(statement -> builder.add(statement.getSubject(), statement.getPredicate(), statement.getObject()));

        Map<UaObjectTypeNode, List<ReferenceDescription>> objectTypes = ctx.getNodeRefMap().entrySet().stream().filter(k -> k.getKey().getNodeClass().equals(NodeClass.ObjectType)).collect(Collectors.toMap(e -> (UaObjectTypeNode) e.getKey(), e -> e.getValue()));
        transformObjectTypeNodes(objectTypes).forEach(statement -> builder.add(statement.getSubject(), statement.getPredicate(), statement.getObject()));

        Map<UaVariableNode, List<ReferenceDescription>> variables = ctx.getNodeRefMap().entrySet().stream().filter(k -> k.getKey().getNodeClass().equals(NodeClass.Variable)).collect(Collectors.toMap(e -> (UaVariableNode) e.getKey(), e -> e.getValue()));
        transformVariableNodes(variables).forEach(statement -> builder.add(statement.getSubject(), statement.getPredicate(), statement.getObject()));

        Map<UaVariableTypeNode, List<ReferenceDescription>> variableTypes = ctx.getNodeRefMap().entrySet().stream().filter(k -> k.getKey().getNodeClass().equals(NodeClass.VariableType)).collect(Collectors.toMap(e -> (UaVariableTypeNode) e.getKey(), e -> e.getValue()));
        transformVariableTypeNodes(variableTypes).forEach(statement -> builder.add(statement.getSubject(), statement.getPredicate(), statement.getObject()));

        Map<UaMethodNode, List<ReferenceDescription>> methods = ctx.getNodeRefMap().entrySet().stream().filter(k -> k.getKey().getNodeClass().equals(NodeClass.Method)).collect(Collectors.toMap(e -> (UaMethodNode) e.getKey(), e -> e.getValue()));
        transformMethodNodes(methods).forEach(statement -> builder.add(statement.getSubject(), statement.getPredicate(), statement.getObject()));

        Map<UaReferenceTypeNode, List<ReferenceDescription>> referenceTypes = ctx.getNodeRefMap().entrySet().stream().filter(k -> k.getKey().getNodeClass().equals(NodeClass.ReferenceType)).collect(Collectors.toMap(e -> (UaReferenceTypeNode) e.getKey(), e -> e.getValue()));
        transformReferenceTypeNodes(referenceTypes).forEach(statement -> builder.add(statement.getSubject(), statement.getPredicate(), statement.getObject()));

        Map<UaViewNode, List<ReferenceDescription>> views = ctx.getNodeRefMap().entrySet().stream().filter(k -> k.getKey().getNodeClass().equals(NodeClass.View)).collect(Collectors.toMap(e -> (UaViewNode) e.getKey(), e -> e.getValue()));
        transformViewNodes(views).forEach(statement -> builder.add(statement.getSubject(), statement.getPredicate(), statement.getObject()));

        Map<UaDataTypeNode, List<ReferenceDescription>> dataTypes = ctx.getNodeRefMap().entrySet().stream().filter(k -> k.getKey().getNodeClass().equals(NodeClass.DataType)).collect(Collectors.toMap(e -> (UaDataTypeNode) e.getKey(), e -> e.getValue()));
        transformDataTypeNodes(dataTypes).forEach(statement -> builder.add(statement.getSubject(), statement.getPredicate(), statement.getObject()));

        ctx.getClient().disconnect();
        return transformEndpointsAndAttach(builder, this.entryPoint).build();
    }

    public ModelBuilder transformEndpointsAndAttach(ModelBuilder builder, Resource entryPoint) {
        builder.add(entryPoint, v.createIRI("http://iwu.fraunhofer.de/c32/hasOpcuaAddressSpace"), v.createIRI("http://opcfoundation.org/UA/84"));
        builder.add(entryPoint, v.createIRI("http://iwu.fraunhofer.de/c32/hasEndpoints"), v.createBNode("endpoints"));
        ctx.getEndpoints().forEach(endp -> {
            IRI endpointInQuestion = TransformerUtils.getIriFromEndpointDescription(endp);
            builder.add(v.createBNode("endpoints"),
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/hasEndpoint"),
                    endpointInQuestion);
            builder.add(endpointInQuestion,
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/hasEndpointUrl"),
                    v.createLiteral(endp.getEndpointUrl()));

            // Server ApplicationDescription
            builder.add(endpointInQuestion,
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/hasServer"),
                    v.createBNode("serverApplicationDescription")
            );
            builder.add(v.createBNode("serverApplicationDescription"),
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/applicationUri"),
                    v.createLiteral(endp.getServer().getApplicationUri())
            );
            builder.add(v.createBNode("serverApplicationDescription"),
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/productUri"),
                    v.createLiteral(endp.getServer().getProductUri())
            );
            builder.add(v.createBNode("serverApplicationDescription"),
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/applicationName"),
                    TransformerUtils.getOptionalLiteralFromLocalizedText(endp.getServer().getApplicationName()).get()
            );
            builder.add(v.createBNode("serverApplicationDescription"),
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/applicationType"),
                    v.createLiteral(endp.getServer().getApplicationType().name())
            );
            Optional.ofNullable(endp.getServer().getGatewayServerUri()).ifPresent(gsu -> builder.add(v.createBNode("serverApplicationDescription"),
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/gatewayServerUri"),
                    v.createLiteral(gsu))
            );

            Optional.ofNullable(endp.getServer().getDiscoveryProfileUri()).ifPresent(dpu -> builder.add(v.createBNode("serverApplicationDescription"),
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/discoveryProfileUri"),
                    v.createLiteral(dpu))
            );
            Arrays.stream(endp.getServer().getDiscoveryUrls()).forEach(durl -> {
                builder.add(v.createBNode("serverApplicationDescription"),
                        v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/discoveryUrls"),
                        v.createLiteral(durl)
                );
            });
            //End server ApplicationDescription
            builder.add(endpointInQuestion,
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/hasServerCertificate"),
                    v.createLiteral(endp.getServerCertificate().toString()));
            builder.add(endpointInQuestion,
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/hasSecurityMode"),
                    v.createLiteral(endp.getSecurityMode().name()));
            builder.add(endpointInQuestion,
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/hasSecurityPolicyUri"),
                    v.createLiteral(endp.getSecurityPolicyUri()));
            // UserIdentityTokens
            Arrays.stream(endp.getUserIdentityTokens()).forEach(uit -> {
                builder.add(endpointInQuestion,
                        v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/hasUserIdentityToken"),
                        v.createBNode("uit" + uit.hashCode()));
                builder.add(v.createBNode("uit" + uit.hashCode()),
                        v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/policyId"),
                        v.createLiteral(uit.getPolicyId()));
                builder.add(v.createBNode("uit" + uit.hashCode()),
                        v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/tokenType"),
                        v.createLiteral(uit.getTokenType().name()));
                Optional.ofNullable(uit.getIssuedTokenType()).ifPresent(itt -> builder.add(v.createBNode("uit" + uit.hashCode()),
                        v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/issuedTokenType"),
                        v.createLiteral(itt)));

                Optional.ofNullable(uit.getIssuerEndpointUrl()).ifPresent(ieu -> builder.add(v.createBNode("uit" + uit.hashCode()),
                        v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/issuerEndpointUrl"),
                        v.createLiteral(ieu)));

                Optional.ofNullable(uit.getSecurityPolicyUri()).ifPresent(spu -> builder.add(v.createBNode("uit" + uit.hashCode()),
                        v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/securityPolicyUri"),
                        v.createLiteral(spu)));
            });
            //End UserIdentityTokens

            builder.add(endpointInQuestion,
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/hasTransportProfileUri"),
                    v.createLiteral(endp.getTransportProfileUri()));
            builder.add(endpointInQuestion,
                    v.createIRI("http://iwu.fraunhofer.de/c32/ua/rdf/hasSecurityLevel"),
                    v.createLiteral(endp.getSecurityLevel().intValue()));
        });
        return builder;

    }

    private List<Statement> transformObjectNodes(Map<UaObjectNode, List<ReferenceDescription>> objectNodes) {
        Function<Map.Entry<UaObjectNode, List<ReferenceDescription>>, Stream<Statement>> pred = e -> {
            List<Statement> statements = transformGenericNode(e.getKey(), e.getValue());
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.EventNotifier), v.createLiteral(e.getKey().getEventNotifier().intValue())));
            return statements.stream();
        };
        return objectNodes.entrySet().stream().flatMap(pred).collect(Collectors.toList());
    }

    private List<Statement> transformObjectTypeNodes(Map<UaObjectTypeNode, List<ReferenceDescription>> objectTypeNodes) {
        Function<Map.Entry<UaObjectTypeNode, List<ReferenceDescription>>, Stream<Statement>> pred = e -> {
            List<Statement> statements = transformGenericNode(e.getKey(), e.getValue());
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.IsAbstract), v.createLiteral(e.getKey().getIsAbstract())));
            return statements.stream();
        };
        return objectTypeNodes.entrySet().stream().flatMap(pred).collect(Collectors.toList());
    }

    private List<Statement> transformVariableNodes(Map<UaVariableNode, List<ReferenceDescription>> variableNodes) {
        Function<Map.Entry<UaVariableNode, List<ReferenceDescription>>, Stream<Statement>> pred = e -> {
            List<Statement> statements = transformGenericNode(e.getKey(), e.getValue());
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.DataType), TransformerUtils.getIriFromNodeId(e.getKey().getDataType(), ctx.getNamespaces())));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.ValueRank), v.createLiteral(e.getKey().getValueRank())));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.AccessLevel), v.createLiteral(e.getKey().getAccessLevel().intValue())));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.UserAccessLevel), v.createLiteral(e.getKey().getUserAccessLevel().intValue())));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.Historizing), v.createLiteral(e.getKey().getHistorizing())));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.ArrayDimensions), v.createLiteral(Arrays.toString(e.getKey().getArrayDimensions()))));


            DataTypeMapper dtm = new DataTypeMapper(dataTypeTree, ctx);
            Class<?> backingClass = dataTypeTree.getBackingClass(e.getKey().getDataType());
            if (backingClass.equals(UNumber.class)) {
                backingClass = UInteger.class;
            } else if (backingClass.equals(Number.class)) {
                backingClass = Integer.class;
            } else if (backingClass.equals(Object.class)) {
                logger.info("found a backingclass Object<?>");
            }

            int builtinTypeId = BuiltinDataType.getBuiltinTypeId(backingClass);
            Optional<List<Value>> valueFromDataValue = TransformerUtils.createValueFromDataValue(e.getKey().getValue(), builtinTypeId, dtm);
            valueFromDataValue.ifPresent(l ->
                    l.stream().filter(Objects::nonNull).forEach(en -> statements.add(v.createStatement(
                            currentSubject,
                            TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.Value),
                            en))));

            return statements.stream();
        };

        return variableNodes.entrySet().stream().flatMap(pred).collect(Collectors.toList());
    }

    private List<Statement> transformVariableTypeNodes(Map<UaVariableTypeNode, List<ReferenceDescription>> variableTypeNodes) {
        Function<Map.Entry<UaVariableTypeNode, List<ReferenceDescription>>, Stream<Statement>> pred = e -> {
            List<Statement> statements = transformGenericNode(e.getKey(), e.getValue());
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.DataType), TransformerUtils.getIriFromNodeId(e.getKey().getDataType(), ctx.getNamespaces())));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.ValueRank), v.createLiteral(e.getKey().getValueRank())));

            DataTypeMapper dtm = new DataTypeMapper(dataTypeTree, ctx);
            Class<?> backingClass = dataTypeTree.getBackingClass(e.getKey().getDataType());
            logger.info("is backingclass: " + backingClass + ", name: " + e.getKey().getDisplayName().toString());
            if (backingClass.equals(UNumber.class)) {
                backingClass = UInteger.class;
            } else if (backingClass.equals(Number.class)) {
                backingClass = Integer.class;
            }

            int builtinTypeId = BuiltinDataType.getBuiltinTypeId(backingClass);
            Optional<List<Value>> valueFromDataValue = TransformerUtils.createValueFromDataValue(e.getKey().getValue(), builtinTypeId, dtm);
            valueFromDataValue.ifPresent(l ->
                    l.stream().filter(Objects::nonNull).forEach(en -> statements.add(v.createStatement(
                            currentSubject,
                            TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.Value),
                            en))));

            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.ArrayDimensions), v.createLiteral(Arrays.toString(e.getKey().getArrayDimensions()))));

            return statements.stream();
        };


        return variableTypeNodes.entrySet().stream().flatMap(pred).collect(Collectors.toList());
    }

    private List<Statement> transformMethodNodes(Map<UaMethodNode, List<ReferenceDescription>> methodNodes) {
        Function<Map.Entry<UaMethodNode, List<ReferenceDescription>>, Stream<Statement>> pred = e -> {
            List<Statement> statements = transformGenericNode(e.getKey(), e.getValue());
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.Executable), v.createLiteral(e.getKey().isExecutable())));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.UserExecutable), v.createLiteral(e.getKey().isUserExecutable())));
            return statements.stream();
        };
        return methodNodes.entrySet().stream().flatMap(pred).collect(Collectors.toList());
    }

    private List<Statement> transformDataTypeNodes(Map<UaDataTypeNode, List<ReferenceDescription>> dataTypeNodes) {
        Function<Map.Entry<UaDataTypeNode, List<ReferenceDescription>>, Stream<Statement>> pred = e -> {
            List<Statement> statements = transformGenericNode(e.getKey(), e.getValue());
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.IsAbstract), v.createLiteral(e.getKey().getIsAbstract())));
            return statements.stream();
        };
        return dataTypeNodes.entrySet().stream().flatMap(pred).collect(Collectors.toList());
    }

    private List<Statement> transformReferenceTypeNodes(Map<UaReferenceTypeNode, List<ReferenceDescription>> referenceTypeNodes) {
        Function<Map.Entry<UaReferenceTypeNode, List<ReferenceDescription>>, Stream<Statement>> pred = e -> {
            List<Statement> statements = transformGenericNode(e.getKey(), e.getValue());
            TransformerUtils.getOptionalLiteralFromLocalizedText(e.getKey().getInverseName()).ifPresent(invName -> statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.InverseName), invName)));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.IsAbstract), v.createLiteral(e.getKey().getIsAbstract())));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.Symmetric), v.createLiteral(e.getKey().getSymmetric())));
            return statements.stream();
        };
        return referenceTypeNodes.entrySet().stream().flatMap(pred).collect(Collectors.toList());
    }

    private List<Statement> transformViewNodes(Map<UaViewNode, List<ReferenceDescription>> viewNodes) {
        Function<Map.Entry<UaViewNode, List<ReferenceDescription>>, Stream<Statement>> pred = e -> {
            List<Statement> statements = transformGenericNode(e.getKey(), e.getValue());
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.ContainsNoLoops), v.createLiteral(e.getKey().getContainsNoLoops())));
            statements.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.EventNotifier), v.createLiteral(e.getKey().getEventNotifier().intValue())));
            return statements.stream();
        };
        return viewNodes.entrySet().stream().flatMap(pred).collect(Collectors.toList());
    }

    private List<Statement> transformGenericNode(UaNode node, List<ReferenceDescription> references) {
        ArrayList<Statement> l = new ArrayList<>();
        currentSubject = TransformerUtils.getIriFromNodeId(node.getNodeId(), ctx.getNamespaces());

        l.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.NodeClass), TransformerUtils.getIriFromNodeClass(node.getNodeClass())));
        TransformerUtils.getOptionalLiteralFromLocalizedText(node.getDisplayName()).ifPresent(literal -> l.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.DisplayName), literal)));
        l.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.BrowseName), v.createLiteral(node.getBrowseName().toParseableString())));
        l.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.NodeId), v.createLiteral(node.getNodeId().toParseableString())));
        TransformerUtils.getOptionalLiteralFromLocalizedText(node.getDescription()).ifPresent(literal -> l.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.Description), literal)));

        // TODO check what happens here when optional attributes not used
        l.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.UserWriteMask), v.createLiteral(node.getUserWriteMask().intValue())));
        l.add(v.createStatement(currentSubject, TransformerUtils.getIriFromAttributeMask(NodeAttributesMask.WriteMask), v.createLiteral(node.getWriteMask().intValue())));

        references.forEach(r -> {
            r.getNodeId().toNodeId(ctx.getNamespaces()).ifPresentOrElse(target -> {
                IRI predicate = TransformerUtils.getIriFromNodeId(r.getReferenceTypeId(), ctx.getNamespaces());
                if (r.getIsForward()) {
                    l.add(v.createStatement(currentSubject, predicate, TransformerUtils.getIriFromNodeId(target, ctx.getNamespaces())));
                } else {
                    l.add(v.createStatement(TransformerUtils.getIriFromNodeId(target, ctx.getNamespaces()), predicate, currentSubject));
                }
            }, () -> logger.info("The target Node with " + r.getNodeId().toParseableString() + "could not be found. Connection with " + node.getNodeId().toParseableString()));
        });
        return l;
    }
}
