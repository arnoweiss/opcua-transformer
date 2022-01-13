package de.fraunhofer.iwu.opcua.util;

import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;

public class TransformerUtils {
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

    public static String getUriFromNodeId(NodeId nodeId, NamespaceTable nst) {
        String namespace = nst.getUri(nodeId.getNamespaceIndex());
        if (!namespace.endsWith("/")){
            namespace += "/";
        }
        String identifier = nodeId.getIdentifier().toString();
        return namespace + identifier;
    }
}
