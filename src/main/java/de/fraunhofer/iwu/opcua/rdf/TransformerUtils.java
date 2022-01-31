package de.fraunhofer.iwu.opcua.rdf;

import org.eclipse.milo.opcua.stack.core.BuiltinDataType;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeAttributesMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Stream;


public class TransformerUtils {

    private final static ValueFactory v = new ValidatingValueFactory();

    public static String getLastIdentifierSegment(String string) throws RuntimeException {
        String parsedUri = string;
        if (!string.contains("/")) {
            if (string.startsWith("urn") & string.contains(":")) {
                String parsedUrn;
                if (string.endsWith(":")) {
                    parsedUrn = string.substring(0, string.length() - 1);
                } else {
                    parsedUrn = string;
                }
                return parsedUrn.substring(parsedUrn.lastIndexOf(":"));
            } else {
                throw new RuntimeException(string + " is no proper uri or urn");
            }
        }
        if (string.endsWith("/")) {
            parsedUri = string.substring(0, string.length() - 1);
        }
        if (string.contains("#")) {
            parsedUri = string.substring(0, string.lastIndexOf("#"));
        }
        return parsedUri.substring(parsedUri.lastIndexOf("/") + 1);
    }

    public static String getUriFromNodeId(NodeId nodeId, NamespaceTable uaNst) {
        Optional<String> namespace = Optional.ofNullable(uaNst.getUri(nodeId.getNamespaceIndex()));
        return namespace.map(n -> {
            String ns = (!n.endsWith("/")) ? (n + "/") : n;
            return ns + nodeId.getIdentifier().toString();
        }).orElseGet(() -> "http://iwu.fraunhofer.de/UA/FallbackNs/"
                + LocalDateTime.now().hashCode()+"/"+nodeId.getIdentifier().toString());
    }

    public static IRI getIriFromNodeId(NodeId nodeId, NamespaceTable uaNst) {
        String uncheckedNsUri = uaNst.getUri(nodeId.getNamespaceIndex());
        String checkedNsUri = uncheckedNsUri.endsWith("/") ? uncheckedNsUri : uncheckedNsUri + "/";
        String segment = nodeId.getIdentifier().toString();
        return v.createIRI(
                checkedNsUri,
                URLEncoder.encode(segment, StandardCharsets.UTF_8));

    }

    public static Optional<Literal> getOptionalLiteralFromLocalizedText(LocalizedText localizedText) {
        Optional<Literal> literal = Optional.ofNullable(localizedText.getText()).map(text ->
                Optional.ofNullable(localizedText.getLocale()).map(locale ->
                        v.createLiteral(text, locale)
                ).orElseGet(() -> v.createLiteral(text)));
        return literal;

    }

    public static IRI getIriFromAttributeMask(NodeAttributesMask attribute) {
        return v.createIRI("http://opcfoundation.org/UA/Attributes/" + attribute.name());
    }

    public static IRI getIriFromNodeClass(NodeClass id) {
        return v.createIRI("http://opcfoundation.org/UA/NodeClasses/" + id.name());
    }

    public static IRI getIriFromEndpointDescription(EndpointDescription e){
        return v.createIRI(e.getEndpointUrl());
    }



    public static Optional<List<Value>> createValueFromDataValue(DataValue value, int backingDataType, DataTypeMapper mapper) {

        return Optional.ofNullable(value.getValue().getValue()).map(val -> {
                    List<Value> result = new ArrayList<>();
                    Stream<Object> stream;
                    if (value.getValue().getValue().getClass().isArray() && !value.getValue().getValue().getClass().equals(ByteString.class)) {
                        stream = Arrays.stream(((Object[]) value.getValue().getValue()));
                    } else {
                        stream = Stream.of(value.getValue().getValue());
                    }

                    if (NodeId.parse("i=" + backingDataType).equals(Identifiers.Boolean)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromBoolean(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.SByte)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromSByte(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.Byte)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromByte(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.Int16)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromInt16(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.Int32)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromInt32(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.Int64)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromInt64(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.UInt16)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromUInt16(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.UInt32)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromUInt32(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.UInt64)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromUInt64(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.Float)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromFloat(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.Double)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromDouble(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.String)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromString(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.DateTime)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromDateTime(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.Guid)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromGuid(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.ByteString)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromByteString(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.XmlElement)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromXmlElement(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.NodeId)) {
                        stream.forEach(e -> result.add(mapper.getIriFromNodeId(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.ExpandedNodeId)) {
                        stream.forEach(e -> result.add(mapper.getIriFromExpandedNodeId(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.StatusCode)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromStatusCode(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.QualifiedName)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromQualifiedName(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.LocalizedText)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromLocalizedText(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(BuiltinDataType.ExtensionObject.getNodeId())) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromExtensionObject(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(Identifiers.DiagnosticInfo)) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromDiagnosticCode(e)));
                    } else if (NodeId.parse("i=" + backingDataType).equals(BuiltinDataType.Variant.getNodeId())) {
                        stream.forEach(e -> result.add(mapper.getLiteralFromVariant(value)));
                    } else {
                        throw new RuntimeException("Datatype: i=" + backingDataType);
                    }
                    return result;
                }
        );
    }
}
