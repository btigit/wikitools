package neo4j;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.apache.lucene.queryParser.QueryParser;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.rest.graphdb.traversal.RestTraversal;
import org.neo4j.rest.graphdb.traversal.RestTraversalDescription;
import org.neo4j.rest.graphdb.traversal.RestTraversalDescription.ScriptLanguage;

import tools.WikipediaModel;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class Mongo2Neo4j {

    public static final String SERVER_ROOT_URI = "http://10.211.55.11:7474/db/data/";
    public static final String DB_PATH = "data/wiki/";
    public static final String INDEX_NAME = "wiki";
	
	public enum WikiRelationshipTypes implements RelationshipType{	
		LINK,REDIRECT
	}

    /** main */
    @SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();
        
        Mongo m = new Mongo( "10.211.55.11" , 27017 );
        DB db = m.getDB( "wiki" );
        DBCollection coll = db.getCollection("docs");
        
		Map<String, String> config = EmbeddedGraphDatabase.loadConfigurations("conf/neo4j.properties");
		GraphDatabaseService gds = new EmbeddedGraphDatabase(DB_PATH,config);
		registerShutdownHook(gds);
		
		Index<Node> nodeIndex = gds.index().forNodes(INDEX_NAME);

        int nodeTotal = 0;
        int loop = 0;
        int commitInterval = 1000;
        
        while (true) {
        	int startDocId = commitInterval*loop;
        	int endDocId = commitInterval*(loop+1);
        	BasicDBObject query = new BasicDBObject();
        	query.put("docid", new BasicDBObject("$gte",startDocId).append("$lt", endDocId));
        	DBCursor cur = coll.find(query).sort(new BasicDBObject("docid",1));
        	
        	if(cur.size()==0) break;
            long startTime = System.currentTimeMillis();
            
    		WikipediaModel model = null;
    		Transaction tx = gds.beginTx();
    		try {
	            int nodeCount = 0;
    	    	while(cur.hasNext()){
    	            
    	    		DBObject obj = cur.next();
            		model = new WikipediaModel();
            		model.setId((Long) obj.get("docid"));
            		model.setTitle((String) obj.get("title"));
            		model.setTitleAnnotation((String) obj.get("titleAnnotation"));
            		model.setText((String) obj.get("text"));
            		model.setRedirect((Boolean) obj.get("redirect"));
            		model.setRedirectText((String) obj.get("redirectText"));
            		model.setStub((Boolean) obj.get("stub"));
            		model.setDisambiguation((Boolean) obj.get("disambiguation"));
            		model.setCategories((ArrayList<String>) obj.get("categories"));
            		model.setLinks((ArrayList<String>) obj.get("links"));
            		model.setLastModified((Date) obj.get("lastModified"));
            		
                	if (model.isRedirect()){
                		//REDIRECT
                		nodeCount += addRedirectNode(gds, nodeIndex, model);
                	}else{
                		//DOCUMENT
                		nodeCount += addDocumentNode(gds, nodeIndex, model);
                	}
    	    	}
            	tx.success();
            	nodeTotal+=nodeCount;
            	long ellapsedTime = System.currentTimeMillis()-startTime;
            	System.out.println(
            			"docid:"+model.getId()
            			+"\tnodeTotal:"+nodeTotal
            			+"\tellapsed:"+ellapsedTime
            			+"\trate:"+nodeCount*1000L/ellapsedTime
            			);
    		} catch (Exception e) {
    			System.err.println(
    					"docid:"+model.getId()
            			+"\ttype:"+model.getType()
    					+"\ttitle:"+model.getTitle()
    					);
    			e.printStackTrace();
    			break;
    		} finally {
    			tx.finish();
    		}
    		loop++;
        }

        System.out.println((System.currentTimeMillis() - start) + "msec");
    }
     
    public static int addDocumentNode(GraphDatabaseService gds, Index<Node> nodeIndex, WikipediaModel model){
    	int count=1;
    	Node sourceNode = addNode(gds, nodeIndex, model);
    	for (String linkTitle : model.getLinks()) {
			if(model.getTitle().equals(linkTitle)) continue;
    		Node linkNode = getNodeByQuery(nodeIndex,"title",linkTitle).getSingle();
    		if(linkNode==null){
    			WikipediaModel linkModel = new WikipediaModel();
    			linkModel.setId(-1L);
    			linkModel.setTitle(linkTitle);
    			linkModel.setEmpty(true);
    			linkNode = addNode(gds, nodeIndex, linkModel);
    		}
    		sourceNode.createRelationshipTo(linkNode, WikiRelationshipTypes.LINK);
    		count++;
		}
    	return count;
    }
    
    public static int addRedirectNode(GraphDatabaseService gds, Index<Node> nodeIndex, WikipediaModel model){
    	int count=1;
    	Node sourceNode = addNode(gds, nodeIndex, model);
    	
		if(model.getTitle().equals(model.getRedirectText())) return count;
    	Node redirectNode = getNodeByQuery(nodeIndex, "title",model.getRedirectText()).getSingle();
    	
    	if(redirectNode==null){
			WikipediaModel redirectModel = new WikipediaModel();
			redirectModel.setId(-1L);
			redirectModel.setTitle(model.getRedirectText());
			redirectModel.setEmpty(true);
			redirectNode = addNode(gds, nodeIndex, redirectModel);
    	}
		sourceNode.createRelationshipTo(redirectNode, WikiRelationshipTypes.REDIRECT);
    	return count;
    }
    
    public static Node addNode(GraphDatabaseService gds, Index<Node> index, WikipediaModel model){
		Node node = null;
    	if(model.isEmpty()){
			node=gds.createNode();
    		setNodePropertyAndAddIndex(node, index, "docid", model.getId());
    		setNodePropertyAndAddIndex(node, index, "title", model.getTitle());
    		setNodePropertyAndAddIndex(node, index, "type", model.getType());
    	}else{
        	node = getNodeByQuery(index,"title",model.getTitle()).getSingle();
    		if(node==null){
    			//create
    			node=gds.createNode();
        		setNodePropertyAndAddIndex(node, index, "docid", model.getId());
        		setNodePropertyAndAddIndex(node, index, "title", model.getTitle());
        		setNodePropertyAndAddIndex(node, index, "type", model.getType());
        		setNodePropertyAndAddIndex(node, index, "lastModified", model.getLastModified().getTime());
        		setNodePropertyArrayAndAddIndex(node, index, "categories", model.getCategories());
    		}else{
    			//update
    			setNodePropertyAndUpdateIndex(node, index, "docid", model.getId());
    			setNodePropertyAndUpdateIndex(node, index, "title", model.getTitle());
    			setNodePropertyAndUpdateIndex(node, index, "type", model.getType());
				setNodePropertyAndUpdateIndex(node, index, "lastModified", model.getLastModified().getTime());
				setNodePropertyArrayAndUpdateIndex(node, index, "categories", model.getCategories());
    		}
    	}
		return node;
    }
    
    public static Traverser traverseRelationshipsByNodeTitle(Node node, int depth, RelationshipType relationshipType, Direction direction, String title){
    	RestTraversalDescription discription = RestTraversal.description();
    	discription.maxDepth(depth);
    	discription.breadthFirst();
    	discription.relationships(relationshipType, direction);
    	discription.filter(ScriptLanguage.JAVASCRIPT, "position.endNode().getProperty('title').equals('"+title+"')");
    	return discription.traverse(node);
    }
	
	public static void setNodePropertyAndAddIndex(Node node, Index<Node> index, String key, Object value){
		node.setProperty(key, value);
		index.add(node, key, value);
	}
	
	public static void setNodePropertyArrayAndAddIndex(Node node, Index<Node> index, String key, ArrayList<String>values){
		node.setProperty(key, values.toArray(new String[0]));
		for (String value : values) {
			index.add(node, key, value);
		}
	}
	
	public static void setNodePropertyAndUpdateIndex(Node node, Index<Node> index, String key, Object value){
		if(node.hasProperty(key) && node.getProperty(key).equals(value)){
			return;
		}else{
			node.setProperty(key, value);
			index.remove(node, key, value);
			index.add(node, key, value);
		}
	}
	
	public static void setNodePropertyArrayAndUpdateIndex(Node node, Index<Node> index, String key, ArrayList<String>values){
		if(node.hasProperty(key)){
			return;
		}else{
			node.setProperty(key, values.toArray(new String[0]));
			for (String value : values) {
				index.remove(node, key, value);
				index.add(node, key, value);
			}
		}
	}
	
	public static IndexHits<Node> getNode(Index<Node> index, String key, Object value){
		return index.get(key, value);
	}

	public static IndexHits<Node> getNodeByQuery(Index<Node> index, String query){
		return index.query(query);
	}
	
	public static IndexHits<Node> getNodeByQuery(Index<Node> index, String field, String query){
		return index.query(field+":\""+QueryParser.escape(query)+"\"");
	}
	
	public static IndexHits<Relationship> getRelationship(Index<Relationship> index, String key, Object value){
		return index.get(key, value);
	}

	public static IndexHits<Relationship> getRelationshipByQuery(Index<Relationship> index, String field, String query){
		return index.query(field+":\""+QueryParser.escape(query)+"\"");
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
