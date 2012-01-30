package neo4j;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.lucene.queryParser.QueryParser;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.rest.graphdb.RestGraphDatabase;

public class Neo4jRestSample {
	
	public static void main(String[] args) throws ParseException {
		String escapeChars ="[\\\\+\\-\\!\\(\\)\\:\\^\\]\\{\\}\\~\\*\\?]";
		System.out.println("SUPER BELL\"Z".replaceAll(escapeChars, "\\\\$0"));
		System.out.println(QueryParser.escape("SUPER BELL\"Z"));
		
		GraphDatabaseService gds = new RestGraphDatabase("http://10.211.55.11:7474/db/data");		
		Index<Node> index = gds.index().forNodes("wiki");
		
		DateFormat df = new SimpleDateFormat("yyyyMMdd");
		
		Node firstNode = gds.createNode();
		addPropertyAndIndex("id", 100, firstNode, index);
		addPropertyAndIndex("term", "第一ノード", firstNode, index);
		String[] categories1 = {"Category1","Category2"};
		addPropertyArrayIndex("categories", categories1, firstNode, index);
		addPropertyAndIndex("lastUpdated", df.parse("20110101").getTime(), firstNode, index);
		addPropertyAndIndex("exist", true, firstNode, index);
		
		Node secondNode = gds.createNode();
		addPropertyAndIndex("id", 200, secondNode, index);
		addPropertyAndIndex("term", "第二ノード", secondNode, index);
		String[] categories2 = {"Category2","Category3"};
		addPropertyArrayIndex("categories", categories2, secondNode, index);
		addPropertyAndIndex("lastUpdated", df.parse("20120101").getTime(), secondNode, index);
		addPropertyAndIndex("exist", false, secondNode, index);
		
		getAndPrint(index, "term", "第一ノード");
		getAndPrint(index, "id", 9999);
		getAndPrint(index, "exist", false);
		
		queryAndPrint(index, "term:第一ノード");
		queryAndPrint(index, "id:[200 TO 300]");
		queryAndPrint(index, "lastUpdated:["+df.parse("20110101").getTime()+" TO "+df.parse("20111231").getTime()+"]");
		queryAndPrint(index, "categories:Category2");
		
		index.delete();
		firstNode.delete();
		secondNode.delete();
	}
		
	private static void addPropertyAndIndex(String key, Object value, Node node, Index<Node> index){
		node.setProperty(key, value);
		index.add(node, key, value);
	}
	
	private static void addPropertyArrayIndex(String key, Object[] value, Node node, Index<Node> index){
		node.setProperty(key, value);
		for (int i = 0; i < value.length; i++) {
			index.add(node, key, value[i]);
		}
	}
	
	private static void getAndPrint(Index<Node> index, String key, Object value){
		IndexHits<Node> hits;
		hits = index.get(key, value);
		System.out.println("key:"+key+" value:"+value);
		for (Node node : hits) {
			System.out.println(node);
		}
	}
	
	private static void queryAndPrint(Index<Node> index, String query){
		IndexHits<Node> hits;
		hits = index.query(query);
		System.out.println("query:"+query);
		for (Node node : hits) {
			System.out.println(node);
		}
	}

}
