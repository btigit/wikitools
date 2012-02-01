package neo4j;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import tools.WikipediaModel.DocumentType;

public class Neo4jSample {
	
    public static final String DB_PATH = "data/wiki/";
    public static final String INDEX_NAME = "wiki";
    	
	public static void main(String[] args) {
		Map<String, String> config = EmbeddedGraphDatabase.loadConfigurations("conf/neo4j.properties");
		GraphDatabaseService gds = new EmbeddedGraphDatabase(DB_PATH,config);
		registerShutdownHook(gds);
		Index<Node> nodeIndex = gds.index().forNodes(INDEX_NAME);
		
		IndexHits<Node> nodes = Mongo2Neo4j.getNode(nodeIndex, "type", DocumentType.DOCUMENT.toString());
		System.out.println(DocumentType.DOCUMENT.toString()+"\t:"+nodes.size());
		nodes = Mongo2Neo4j.getNode(nodeIndex, "type", DocumentType.DISAMBIGUATION.toString());
		System.out.println(DocumentType.DISAMBIGUATION.toString()+"\t:"+nodes.size());
		nodes = Mongo2Neo4j.getNode(nodeIndex, "type", DocumentType.STUB.toString());
		System.out.println(DocumentType.STUB.toString()+"\t:"+nodes.size());
		nodes = Mongo2Neo4j.getNode(nodeIndex, "type", DocumentType.EMPTY.toString());
		System.out.println(DocumentType.EMPTY.toString()+"\t:"+nodes.size());
		nodes = Mongo2Neo4j.getNode(nodeIndex, "docid", -1);
		System.out.println("docid(-1)\t:"+nodes.size());
	}

	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running example before it's completed)
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    } );
	}
}
