package de.fraunhofer.iwu.opcua.util;

import com.digitalpetri.strictmachine.Fsm;
import org.eclipse.milo.opcua.sdk.client.AddressSpace;
import org.eclipse.milo.opcua.sdk.client.DataTypeTreeSessionInitializer;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfig;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.client.session.SessionFsm;
import org.eclipse.milo.opcua.stack.client.DiscoveryClient;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.NamespaceTable;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.enumerated.BrowseDirection;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

public class OpcuaContext {

    final Logger logger = LoggerFactory.getLogger(getClass());
    private OpcUaClient client;
    private Map<? extends UaNode, List<ReferenceDescription>> nodeRefMap;
    private List<EndpointDescription> endpoints;
    private NamespaceTable nst;
    private AddressSpace as;
    private AddressSpace.BrowseOptions hierarchicalOptions;
    private AddressSpace.BrowseOptions allNodesOptions;


    public OpcuaContext(String endpointUrl) {
        try {
            this.endpoints = browseEndpoints(endpointUrl);
            this.client = createClient(endpointUrl);
            nst = getClient().getNamespaceTable();
            as = getClient().getAddressSpace();
            hierarchicalOptions = AddressSpace.BrowseOptions.builder().setBrowseDirection(BrowseDirection.Forward).setIncludeSubtypes(true).setReferenceType(Identifiers.HierarchicalReferences).setNodeClassMask(uint(0)).build();
            allNodesOptions = AddressSpace.BrowseOptions.builder().setBrowseDirection(BrowseDirection.Forward).setIncludeSubtypes(true).setReferenceType(Identifiers.References).setNodeClassMask(uint(0)).build();
            UaNode rootNode = as.getNode(Identifiers.RootFolder);
            this.nodeRefMap = browseAddressSpace(rootNode, new ConcurrentHashMap<UaNode, List<ReferenceDescription>>());

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private List<EndpointDescription> browseEndpoints(String endpointUrl) throws InterruptedException, ExecutionException {
        return DiscoveryClient.getEndpoints(endpointUrl + "/discovery").get();
    }

    private Map<? extends UaNode, List<ReferenceDescription>> browseAddressSpace(UaNode root, Map<UaNode, List<ReferenceDescription>> nodeMap) throws UaException {
        Map<UaNode, List<ReferenceDescription>> localMap = nodeMap;
        as.setBrowseOptions(allNodesOptions);
        List<ReferenceDescription> allRefs = as.browse(root);
        as.setBrowseOptions(hierarchicalOptions);
        if (!nodeMap.containsKey(root)) {
            localMap.put(root, allRefs);
            List<ReferenceDescription> hierRefs = as.browse(root);
            hierRefs.forEach(r -> {
                try {
                    UaNode node = as.getNode(r.getNodeId().toNodeId(nst).get());
                    localMap.put(node, browseAddressSpace(node, localMap).get(node));
                } catch (UaException e) {
                    logger.info("unable to fetch node " + r.getNodeId().toParseableString() + ". Skipping.");
                }
            });
        }
        return localMap;
    }

    private OpcUaClient createClient(String endpointUrl) throws UaException, ExecutionException, InterruptedException {
        SecurityPolicy securityPolicy = SecurityPolicy.None;
        EndpointDescription endpoint = endpoints.stream().filter(e -> e.getSecurityPolicyUri().equals(securityPolicy.getUri())).findFirst().orElseGet(() -> {
            throw new RuntimeException("No endpoints matching filter");
        });
        logger.info("Using Endpoint with " + endpoint.getEndpointUrl() + " at " + String.join(",", endpoint.getServer().getDiscoveryUrls()));

        OpcUaClientConfig clientConfig = OpcUaClientConfig.builder().setApplicationName(LocalizedText.english("AddressSpace RDF-Transformer")).setApplicationUri("http://iwu.fraunhofer.de/C32/AddressSpaceTransformer").setEndpoint(endpoint).setRequestTimeout(uint(5000)).setIdentityProvider(new AnonymousProvider()).build();


        OpcUaClient opcUaClient = OpcUaClient.create(clientConfig);
        opcUaClient.addSessionInitializer(new DataTypeTreeSessionInitializer());
        opcUaClient.connect().get();
        return opcUaClient;
    }

    public Map<? extends UaNode, List<ReferenceDescription>> getNodeRefMap() {
        return nodeRefMap;
    }

    public List<EndpointDescription> getEndpointDescriptions() {
        return endpoints;
    }

    public OpcUaClient getClient() {
        return client;
    }

    public List<EndpointDescription> getEndpoints() {
        return endpoints;
    }

    public NamespaceTable getNamespaces() {
        return nst;
    }
}