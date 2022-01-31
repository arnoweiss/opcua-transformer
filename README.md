# OPC UA Transformer
The OPC UA Transformer project serves to transform the nodes on a live OPC UA server to data strucutres. For  communication
with the server, the tool uses the Eclipse Milo Stack that currently supports OPC UA v1.03. The transformer currently only
implements the transformation for pure RDF, it is extensible however.

## Target Formats

### Pure RDF
The Transformation to pure RDF serves to integrate the nodeset structure of a live server into a knowledge graph. It preserves
the semantics of OPC UA and uses no higher-level ontologies. The server structure can be plugged into an existing graph when 
specifying an adaption point. The test class RdfTransformerTest.java uses an AssetAdministrationShell (V3RC01) as the adaption 
point. 

## Usage
The project can be compiled with `mvn install` and integrated as a dependency. Since a OPC UA example
server is started and browsed during the build, this may take a while. Afterwards the lib can be integrated into any 
maven/gradle project.