package mmdb.kvs.app.benchmarking;


	import org.neo4j.driver.v1.AuthTokens;
	import org.neo4j.driver.v1.Config;
	import org.neo4j.driver.v1.Driver;
	import org.neo4j.driver.v1.GraphDatabase;
	import org.neo4j.driver.v1.Session;
	import org.neo4j.driver.v1.StatementResult;
	import org.neo4j.graphdb.GraphDatabaseService;
	import org.neo4j.graphdb.Node;
	import org.neo4j.graphdb.Relationship;
	import org.neo4j.graphdb.RelationshipType;
	import org.neo4j.graphdb.factory.GraphDatabaseFactory;
	import org.neo4j.graphdb.factory.GraphDatabaseSettings;
	import org.neo4j.graphdb.Transaction;

	import com.graphaware.runtime.DatabaseRuntime;
	import com.graphaware.runtime.GraphAwareRuntime;
	import com.graphaware.runtime.GraphAwareRuntimeFactory;
	//import com.graphaware.runtime.*;


	import com.graphaware.module.es.mapping.Mapping;
	import com.graphaware.module.es.util.ServiceLoader;

	import com.graphaware.common.uuid.*;
	import com.graphaware.module.uuid.UuidConfiguration;
	import com.graphaware.module.uuid.UuidModule;
	import com.graphaware.module.es.*;
	import com.fasterxml.jackson.core.JsonParser;
	import com.google.gson.JsonElement;
	import com.google.gson.JsonObject;
	import com.graphaware.common.*;


	//import org.junit.Assert.*;
	//import org.junit.Test;

	import static org.junit.Assert.assertEquals;

	import java.io.File;
	import java.net.InetAddress;
	import java.util.HashMap;
	import java.util.List;
	import java.util.Map;

	import org.neo4j.codegen.*;
	import org.neo4j.consistency.checking.cache.CacheAccess.Client;
	import org.neo4j.driver.*;

	import org.neo4j.graphdb.*;
	import org.neo4j.*;

	public class neo {
		
		
		protected static final String HOST = "localhost";
		protected static final String PORT = "9200";
		private static ElasticSearchConfiguration configuration;
		private static GraphDatabaseService graphDb;
		//make sure Neo4j is shut down properly 
		private static void registerShutdownHook( final GraphDatabaseService graphDb )
		{
			Runtime.getRuntime().addShutdownHook( new Thread()
				{
				@Override
				public void run()
				{
					//shutdown the db when jvm exits
				graphDb.shutdown();
				}
			
				}
			);
		}
		

		//CReate Relationship types using an enum
		private static enum RelTypes implements RelationshipType
		{
		    KNOWS
		}
		
		public static void main(String[] args){ 
		{
			
		//Driver driver = GraphDatabase.driver("bolt://localhost:7687",config);
		Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4j1"));
		
		
	String query = "MATCH (:Message {creationDate:{creationDate}})<-[:HAS_CREATOR]-(a:Comment) RETURN a.browser as browser";
	//String query = args[0]
		//String query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match_all\\\":{}}}') YIELD node RETURN node.name";
		Map<String, Object> queryMap = new HashMap<>();
	   queryMap.put("creationDate","2011");
		try (Session session = driver.session()) 
		{

		    //String query = null;
			StatementResult result = session.run(query, queryMap);
			//StatementResult result = session.run(query);
		    while (result.hasNext()) 
		    {
		    	
		    	System.out.println(result.next());
		        //System.out.println(result.next().get("browser"));
		    }
		}
		
		
		//query = args[0];
		
		
		
		
		
		//String query = "CALL ga.es.queryRelationshipRaw('{\\\"query\\\":{\\\"match_all\\\":{}}}') YIELD json RETURN json";
		//System.out.println(args);
	    //esquery relationship
	    //query = "CALL ga.es.queryRelationshipRaw('{\\\"query\\\":{\\\"match_all\\\":{}}}') YIELD json RETURN json";
		
		
		//esquery relationship of size 100
	    //query = "CALL ga.es.queryRelationshipRaw('{\\\"query\\\":{\\\"match_all\\\":{}}, \\\"size\\\":100}') YIELD json RETURN json";
		
		
		
		//Get the node(default 10 nodes)
	   // query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match_all\\\":{}}}') YIELD node RETURN node.name";
		
		
		
		
	    query = "CALL ga.es.queryNode('{\"query\":{\"range\":{\"creationDate\":{\"lt\":20110817111021570}}},\"size\":100}') YIELD node RETURN toFloat(count(node)) AS messageCount,CASE\n" + 
	    		"    WHEN node.length <  40 THEN 0\n" + 
	    		"    WHEN node.length <  80 THEN 1\n" + 
	    		"    WHEN node.length < 160 THEN 2\n" + 
	    		"    ELSE                           3\n" + 
	    		"  END AS lengthCategory,sum(node.length) AS sumMessageLength,avg(node.length) AS averageMessageLength,"
	    		+ "count(node) AS totalMessageCount,node.length AS MessageLength,(node.creationDate/10000000000000) AS year,"
	    		+ "node:Comment AS isComment,node.totalMessageCount/(node.messageCount)*100 as percentOfMessages,"
	    		+ "node:name  ORDER BY year DESC, isComment ASC";

	    

	    	

	    	
	    
//	    query = "CALL ga.es.queryNode('')   YIELD node MATCH (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_CREATOR]-(message:Message)-[:HAS_TAG]->(tag:Tag)
	//WHERE message.creationDate >= 20091231230000000
	//  AND message.creationDate <= 20101107230000000
	//  AND (country.name = 'Ethiopia' OR country.name = 'Belarus')
	//WITH
	//  country.name AS countryName,
	//  message.creationDate/100000000000%100 AS month,
	//  person.gender AS gender,
	//  floor((20130101 - person.birthday) / 10000 / 5.0) AS ageGroup,
	//  tag.name AS tagName,
	//  message
	//WITH
	//  countryName, month, gender, ageGroup, tagName, count(message) AS messageCount
	//WHERE messageCount > 100
	//RETURN
	//  countryName,
	//  month,
	//  gender,
	//  ageGroup,
	//  tagName,
	//  messageCount
	//ORDER BY
	//  messageCount DESC,
	//  tagName ASC,
	//  ageGroup ASC,
	//  gender ASC,
	//  month ASC,
	//  countryName ASC
	//LIMIT 100"
	//	
	    //esquery node and get specific number of nodes with labels of node 
	    //query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match_all\\\":{}}, \\\"size\\\":100}') YIELD node RETURN labels(node), count(node), ORDER BY count(node)";
	   
	    //return person born date in descending order
	   //query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match\\\":{\\\"_type\\\":\\\"Person\\\"}}, \\\"size\\\":100}') YIELD node RETURN node.birthday, count(node) ORDER BY count(node) DESC";
	    
		
		//REturns person name and born date 
		//query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match\\\":{\\\"_type\\\":\\\"Person\\\"}}, \\\"size\\\":100}') YIELD node RETURN node.name,node.born, count(node) ORDER BY count(node)";
		
		//query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match\\\":{\\\"_type\\\":\\\"Person\\\"}}, \\\"size\\\":100}') YIELD node RETURN node.name, count(node) ORDER BY count(node)";
		//query = "CALL ga.es.queryRelationshipRaw('{\\\"query\\\":{\\\"match\\\":{\\\"roles\\\":\\\"Mom\\\"}}}') YIELD json RETURN json";	
		
		//Get the node count 
		//query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match_all\\\":{}}}') YIELD node RETURN count(node)";
	    
		//return title of the MOvie
		//query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match\\\":{\\\"_type\\\":\\\"Movie\\\"}}, \\\"size\\\":100}') YIELD node RETURN node.title";
		
		
		//returns collection of movie titles
		//query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match\\\":{\\\"_type\\\":\\\"Movie\\\"}}, "
			//	+ "\\\"size\\\":100}') YIELD node RETURN collect(node.title)";
		
		//collects person name and born date
		//query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match\\\":{\\\"_type\\\":\\\"Person\\\"}}, "
			//	+ "\\\"size\\\":100}') YIELD node RETURN collect(node.name)";
		
		
		//Wreturns avg & sum & collect
		/*query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match\\\":{\\\"_type\\\":\\\"Person\\\"}}, "
				+ "\\\"size\\\":100}') YIELD node RETURN avg(node.born),sum(node.born), collect(node.born)";*/
		
		//Wreturns <= range query
		/*query = "CALL ga.es.queryNode('{\"query\": {\"range\" : {\"born\" : {\"lte\" : 1930}}}"
				+ ",\"size\":100}') YIELD node RETURN node.born,node.name";*/
		
		//using optional match(minimum number of required matches)
		/*query = "CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"Tom Tykwer\"}},"
				+ "\"minimum_should_match\": \"1\"}') "
				+ "YIELD node RETURN node.name";*/
		
		/*query = "CALL ga.es.queryNode('{\"query\":{\"name\":\"Tom Tykwer\"}},"
				+ "\"minimum_should_match\": \"1\"}') YIELD node RETURN node.name";*/
		
		//return distinct
		/*query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match\\\":{\\\"_type\\\":\\\"Movie\\\"}}, "
				+ "\\\"size\\\":100}') YIELD node RETURN DISTINCT(node.title)";*/

		//PATTERN MATCHING
	     //query = "CALL ga.es.queryNode('{\"query\":{\"regexp\":{\"name\": \"h.*s\"}}},\"size\":100}') YIELD node RETURN node.name";
		
		//No UNWIND in ES but we can restructure the data in nested format to achieve this

		//query = "CALL ga.es.queryrelationshipMapping('{\"query\": { \"properties\" : {\"inVname\" : { \"type\" : \"text\" },\"outVname\":{\"type\" : \"text\"}}}}')";

		
		//Get all nodes with a label 
	    //query = "CALL ga.es.queryNode('{\\\"query\\\":{\\\"match\\\":{\\\"_type\\\":\\\"Movie\\\"}}}') YIELD node RETURN node, movie.title";
		//query = "CALL ga.es.queryNode('{\"query\":{\"match\":{\"name\":\"Tom\"}}}') YIELD node RETURN labels(node)";
		//query = "CALL ga.es.queryRelationshipRaw('{\"query\":{\"match\":{\"roles\":\"Annie\"}}}') YIELD json RETURN json";
		//query = "CALL ga.es.queryNode('{\"query\":{\\\"match\\\":{\\\"name\\\":\\\"Tom\\\"}}}') YIELD node, score RETURN node, score ORDER BY score  LIMIT 3";
		
//		queryMap = new HashMap<>();
//		try (Session session = driver.session()) 
//		{
	//
//		    StatementResult result = session.run(query, queryMap);
//		    while (result.hasNext()) 
//		    {
//		        System.out.println(result.next().asMap().toString());
//		    }
//		}

		GraphDatabaseService graphDb;
		//Starting an embedded database with configuration settings
		String pathdb = "/home/raksha/neo4j-community-3.3.2/data/databases/graphDb";
	    graphDb = new GraphDatabaseFactory()
	.newEmbeddedDatabaseBuilder(new File(pathdb))
	.loadPropertiesFromFile( "/home/raksha/neo4j-community-3.3.2/conf/" + "neo4j.conf" )
	.newGraphDatabase();
	    
	    //Create properties
	   /* IndicesAdminClient indicesAdminClient = client.admin().indices();
	    client.admin().indices().preparePutMapping("neo4j-index-relationship")   
	    .setType("ACTED_IN")                                
	    .setSource("{\n" +                              
	            "  \"properties\": {\n" +
	            "    \"inVname\": {\n" +
	            "    \"outVname\": {\n" +
	            "    \"inVtype\": {\n" +
	            "      \"outVtype\": \"text\"\n" +
	            "    }\n" +
	            "  }\n" +
	            "}")
	    .get();*/
	    
	   

	    
		
		
		//variables to create relationships
		//GraphDatabaseService graphDatabase;
		//Node firstNode;
		//Node secondNode;
		//Relationship relationship;
		
		/*GraphDatabaseFactory graphDbFactory = new GraphDatabaseFactory();
		
		//to open an existing database or create an embedded database
		graphDb = graphDbFactory.newEmbeddedDatabase(new File("/var/lib/neo4j/data/databases/graph.db"));
		registerShutdownHook(graphDb);*/
		
		//reading neo4j transactions from database
		/*try ( Transaction tx = graphDb.beginTx() )
		{
		    // Database operations go here
			firstNode = graphDb.createNode();
			firstNode.setProperty( "message", "Hello, " );
			secondNode = graphDb.createNode();
			secondNode.setProperty( "message", "World!" );

			relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
			relationship.setProperty( "message", "brave Neo4j " );
			
			
			System.out.print( firstNode.getProperty( "message" ) );
			System.out.print( relationship.getProperty( "message" ) );
			System.out.print( secondNode.getProperty( "message" ) );
			
			  // match all nodes
			Result result = graphDb.execute("CALL ga.es.queryNode('{\"query\":{\"match_all\":{}}}') YIELD node return node");
	        ResourceIterator<Node> resIterator = result.columnAs("node");
	        
	        System.out.println("Result of the method " + resIterator.stream().count());
	       //assertEquals(4, resIterator.stream().count());
			tx.success();
			
		} */
		
		
		// match all relationships
	try (Transaction tx = graphDb.beginTx()) {
	      Result result = graphDb.execute ("MATCH (n) RETURN(n)");
	      //ResourceIterator<Relationship> resIterator = result.columnAs("relationship");
	        ResourceIterator<String> resIterator = result.columnAs("n");
	        System.out.println("Result of the method " + resIterator.stream().count());
		 //System.out.println("Result of the method");
	        //assertEquals(3, resIterator.stream().count());

	       tx.success();
	    }
		
	    
	   
	   
	    
		//creating small graph consisting of two nodes
			
	    
	    /*try (Transaction tx = graphDb.beginTx()) {
	        Result result = graphDb.execute("CALL ga.es.initialized() YIELD status return status");

	        List<String> columns = result.columns();
	        assertEquals(columns.size(), 1);

	        Map<String, Object> next = result.next();
	        assertTrue(next.get("status") instanceof Boolean);

	        assertTrue((Boolean) next.get("status"));

	        tx.success();
	    }*/
		
		
		
		//stop the database
			graphDb.shutdown();
		
		//Client client = TransportClient.builder().build()
	   //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9200));

		//configuration = ElasticSearchConfiguration.defaultConfiguration("localhost", "9200");
	    
		/*GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(graphDb); //where database is an instance of GraphDatabaseService
	    runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), graphDb));

	    configuration = ElasticSearchConfiguration.defaultConfiguration();
	    runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

	    runtime.start();
		*/
		
		}
		}
	}



