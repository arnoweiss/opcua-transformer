package de.fraunhofer.iwu.opcua.rdf;

import de.fraunhofer.iwu.opcua.rdf.TransformerUtils;
import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.rdf4j.model.util.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TransformerUtilsTest {

    Map<NodeId, String> nodeIdsMap = new HashMap<>();
    NamespaceTable uaNst = new NamespaceTable();
    List<String> uris = new ArrayList<>();

    @BeforeEach
    void setUp() {
        uaNst.addUri("http://opcfoundation.org/UA/");
        uaNst.addUri("http://opcfoundation.org/UA/DI/");
        uaNst.addUri("http://opcfoundation.org/UA/Machinery");

        nodeIdsMap.put(NodeId.parse("i=5"), "http://opcfoundation.org/UA/5");
        nodeIdsMap.put(NodeId.parse("ns=1;i=15031"), "http://opcfoundation.org/UA/DI/15031");
        nodeIdsMap.put(NodeId.parse("ns=2;i=6001"), "http://opcfoundation.org/UA/Machinery/6001");

        uris.add("http://opcfoundation.org/UA/DI");
        uris.add("http://opcfoundation.org/UA/DI/");
        uris.add("http://opcfoundation.org/UA/DI#Rand");
    }

    @Test
    void getUriFromNodeId() {
        nodeIdsMap.forEach((key, value) -> assertTrue(TransformerUtils.getUriFromNodeId(key, uaNst).equals(value)));
    }

    @Test
    void getIriFromNodeId() {
        nodeIdsMap.forEach((key, value) -> assertEquals(value, TransformerUtils.getIriFromNodeId(key, uaNst).toString()));
        String urn = "urn:eclipse:milo:hello-world/QualifiedName{name=CustomUnionType, namespaceIndex=2}.Description";
    }

    @Test
    void getLastUriSegment() {

        uris.forEach(uri -> {
            assertEquals("DI", TransformerUtils.getLastIdentifierSegment(uri));
        });
    }

    @Test
    void getOptionalLiteralFromLocalizedText() {
        assertEquals(Optional.empty(), TransformerUtils.getOptionalLiteralFromLocalizedText(LocalizedText.NULL_VALUE));
        assertEquals(Optional.empty(), TransformerUtils.getOptionalLiteralFromLocalizedText(new LocalizedText("en", null)));
        assertEquals(Optional.of(Values.literal("text")), TransformerUtils.getOptionalLiteralFromLocalizedText(new LocalizedText(null, "text")));
        assertEquals(Optional.of(Values.literal("text", "en")), TransformerUtils.getOptionalLiteralFromLocalizedText(new LocalizedText("en", "text")));

    }
}