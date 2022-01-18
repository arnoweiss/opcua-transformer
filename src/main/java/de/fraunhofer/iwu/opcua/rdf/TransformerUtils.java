package de.fraunhofer.iwu.opcua.rdf;

import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeAttributesMask;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.Values;

import java.util.Optional;


public class TransformerUtils {

    private final static ValueFactory v = new ValidatingValueFactory();

    public static String getLastUriSegment(String uri) throws RuntimeException {
        String parsed = uri;
        if (!uri.contains("/")) {
            throw new RuntimeException(uri + " is no proper uri");
        }
        if (uri.endsWith("/")) {
            parsed = uri.substring(0, uri.length() - 1);
        }
        if (uri.contains("#")) {
            parsed = uri.substring(0, uri.lastIndexOf("#"));
        }
        return parsed.substring(parsed.lastIndexOf("/") + 1);
    }

    public static String getUriFromNodeId(NodeId nodeId, NamespaceTable uaNst) {
        String namespace = uaNst.getUri(nodeId.getNamespaceIndex());
        namespace = (!namespace.endsWith("/")) ? (namespace + "/") : namespace;
        String identifier = nodeId.getIdentifier().toString();
        return namespace + identifier;
    }

    public static IRI getIriFromNodeId(NodeId nodeId, NamespaceTable uaNst) {
        String uncheckedNsUri = uaNst.getUri(nodeId.getNamespaceIndex());
        String checkedNsUri = uncheckedNsUri.endsWith("/") ? uncheckedNsUri : uncheckedNsUri + "/";
        return Values.iri(checkedNsUri, nodeId.getIdentifier().toString());
    }

    public static Optional<Literal> getOptionalLiteralFromLocalizedText(LocalizedText localizedText) {
        Optional<Literal> literal = Optional.ofNullable(localizedText.getText()).map(text ->
                Optional.ofNullable(localizedText.getLocale()).map(locale ->
                        Values.literal(text, locale)
                ).orElseGet(() -> Values.literal(text)));
        return literal;

    }

    public static IRI getIriFromAttributeMask(NodeAttributesMask attribute){
        return v.createIRI("http://opcfoundation.org/UA/Attributes/" + attribute.name());
    }

    public static IRI getIriFromNodeClass(NodeClass id) {
        return v.createIRI("http://opcfoundation.org/UA/NodeClasses/" + id.name());
    }
}
