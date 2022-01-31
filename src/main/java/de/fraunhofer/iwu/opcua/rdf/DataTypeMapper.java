package de.fraunhofer.iwu.opcua.rdf;

import de.fraunhofer.iwu.opcua.util.OpcuaContext;
import org.eclipse.milo.opcua.sdk.client.DataTypeTreeSessionInitializer;
import org.eclipse.milo.opcua.sdk.core.DataTypeTree;
import org.eclipse.milo.opcua.stack.core.BuiltinDataType;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class DataTypeMapper {
    private final DataTypeTree tree;
    private final OpcuaContext ctx;
    private final ValueFactory v;

    public DataTypeMapper(DataTypeTree dataTypeTree, OpcuaContext opcuaContext) {
        tree = dataTypeTree;
        v = new ValidatingValueFactory();
        ctx = opcuaContext;
    }

    public Literal getLiteralFromBoolean(Object o) {
        return v.createLiteral(Boolean.parseBoolean(o.toString()));
    }

    public Literal getLiteralFromSByte(Object o) {
        return v.createLiteral(Byte.parseByte(o.toString()));
    }

    public Literal getLiteralFromByte(Object o) {
        return v.createLiteral(Short.parseShort(o.toString()));
    }

    public Literal getLiteralFromInt16(Object o) {
        return v.createLiteral(Short.parseShort(o.toString()));
    }

    public Literal getLiteralFromUInt16(Object o) {
        return v.createLiteral(Integer.parseInt(o.toString()));
    }

    public Literal getLiteralFromInt32(Object o) {
        return v.createLiteral(Integer.parseInt(o.toString()));
    }

    public Literal getLiteralFromUInt32(Object o) {
        return v.createLiteral(Long.parseLong(o.toString()));
    }

    public Literal getLiteralFromInt64(Object o) {
        return v.createLiteral(Long.parseLong(o.toString()));
    }

    public Literal getLiteralFromUInt64(Object o) {
        return v.createLiteral(Long.parseLong(o.toString()));
    }

    public Literal getLiteralFromFloat(Object o) {
        return v.createLiteral(Float.parseFloat(o.toString()));
    }

    public Literal getLiteralFromDouble(Object o) {
        return v.createLiteral(Double.parseDouble(o.toString()));
    }

    public Literal getLiteralFromString(Object o) {
        return v.createLiteral(o.toString());
    }

    public Literal getLiteralFromDateTime(Object o) {
        return v.createLiteral(((DateTime) o).getJavaDate());
    }

    public Literal getLiteralFromGuid(Object o) {
        return v.createLiteral(((UUID) o).toString());
    }

    public Literal getLiteralFromByteString(Object o) {
        return v.createLiteral(Arrays.toString(((ByteString) o).bytes()));
    }

    public Literal getLiteralFromXmlElement(Object o) {
        return v.createLiteral(((XmlElement) o).toString());
    }

    public IRI getIriFromNodeId(Object o) {
        return v.createIRI(TransformerUtils
                .getUriFromNodeId(NodeId.parse(((NodeId) o).toParseableString()), ctx.getNamespaces()));
    }

    public IRI getIriFromExpandedNodeId(Object o) {
        return v.createIRI(TransformerUtils
                .getUriFromNodeId(((ExpandedNodeId) o).toNodeId(ctx.getNamespaces()).get(), ctx.getNamespaces()));
    }

    public Literal getLiteralFromStatusCode(Object o) {
        return v.createLiteral(((StatusCode) o).toString());
    }

    public Literal getLiteralFromQualifiedName(Object o) {
        return v.createLiteral(((QualifiedName) o).getName());
    }

    public Literal getLiteralFromLocalizedText(Object o) {
        String text = ((LocalizedText) o).getText();
        if (text == null) {
            return null;
        } else {
            return v.createLiteral(text);
        }
    }

    public Literal getLiteralFromExtensionObject(Object o) {
        String decode = ((ExtensionObject) o).decode(ctx.getClient().getDynamicSerializationContext()).toString();
        return v.createLiteral(decode);
    }

    public Literal getLiteralFromDiagnosticCode(Object o) {

        return v.createLiteral(((DiagnosticInfo) o).getLocalizedText());
    }

    public Value getLiteralFromVariant(DataValue node) {
        Value ret = null;
        try {
            DataTypeTree dataTypeTree = (DataTypeTree) ctx.getClient().getSession().get().getAttribute(DataTypeTreeSessionInitializer.SESSION_ATTRIBUTE_KEY);
            DataTypeMapper dtm = new DataTypeMapper(dataTypeTree, ctx);
            int builtinTypeId = BuiltinDataType.getBuiltinTypeId(dataTypeTree.getBackingClass(node.getValue().getDataType().get().toNodeId(ctx.getNamespaces()).get()));
            ret = TransformerUtils.createValueFromDataValue(node, builtinTypeId, this).get().get(0);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return ret;

    }
}
