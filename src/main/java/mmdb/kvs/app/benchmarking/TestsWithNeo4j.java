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
import org.neo4j.jmx.JmxUtils;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

import org.neo4j.codegen.*;
import org.neo4j.consistency.checking.cache.CacheAccess.Client;
import org.neo4j.driver.*;

import org.neo4j.graphdb.*;
import org.neo4j.*;

public class TestsWithNeo4j {
	
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
	
	public static void main(String[] args) 
	{
		
	//Driver driver = GraphDatabase.driver("bolt://localhost:7687",config);
	Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j","neo4j1"));
	String query;
	query = "MATCH (:Message {creationDate:{creationDate}})<-[:HAS_CREATOR]-(a:Comment) RETURN a.browserUsed as browser";
	
	//String query =args[0];
	Map<String, Object> queryMap = new HashMap<>();
	queryMap.put("creationDate", "2011");
	try (Session session = driver.session()) 
	{

	    StatementResult result = session.run(query, queryMap);
	    while (result.hasNext()) 
	    {
	        //System.out.println(result.next().get("browser"));
	    	System.out.println(result.next());
	    }
	}
	
	//}

	//public static void Neo4jtest() 
	//{
		/*for(int i=0 ; i<10 ; i++)
		{*/
			
			
			long startTime = System.currentTimeMillis();
		
	 query = "MATCH (message:Message)\n" + 
	   		"WHERE message.creationDate <= 20110817111021570\n" + 
	   		"WITH toFloat(count(message)) AS totalMessageCount // this should be a subquery once Cypher supports it\n" + 
	   		"MATCH (message:Message)\n" + 
	   		"WHERE message.creationDate <= 20110817111021570\n" + 
	   		"  AND message.content IS NOT NULL\n" + 
	   		"WITH\n" + 
	   		"  totalMessageCount,\n" + 
	   		"  message,\n" + 
	   		"  message.creationDate/10000000000000 AS year\n" + 
	   		"WITH\n" + 
	   		"  totalMessageCount,\n" + 
	   		"  year,\n" + 
	   		"  message:Comment AS isComment,\n" + 
	   		"  CASE\n" + 
	   		"    WHEN message.length <  40 THEN 0\n" + 
	   		"    WHEN message.length <  80 THEN 1\n" + 
	   		"    WHEN message.length < 160 THEN 2\n" + 
	   		"    ELSE                           3\n" + 
	   		"  END AS lengthCategory,\n" + 
	   		"  count(message) AS messageCount,\n" + 
	   		"  floor(avg(message.length)) AS averageMessageLength,\n" + 
	   		"  sum(message.length) AS sumMessageLength\n" + 
	   		"RETURN\n" + 
	   		"  year,\n" + 
	   		"  isComment,\n" + 
	   		"  lengthCategory,\n" + 
	   		"  messageCount,\n" + 
	   		"  averageMessageLength,\n" + 
	   		"  sumMessageLength,\n" + 
	   		"  messageCount / totalMessageCount AS percentageOfMessages\n" + 
	   		"ORDER BY\n" + 
	   		"  year DESC,\n" + 
	   		"  isComment ASC,\n" + 
	   		"lengthCategory ASC";
	    
		
		
		
		long endTime = System.currentTimeMillis();
		System.out.println("time taken:"+ (endTime) + "milliseconds");
	    
		//}
		
		//}


		
    
	
	
	
	queryMap = new HashMap<>();
	try (Session session = driver.session()) 
	{

	    StatementResult result = session.run(query, queryMap);
	    while (result.hasNext()) 
	    {
	        System.out.println(result.next().asMap().toString());
	    }
	}

	
	/*public void prepareTestDatabase()
	{*/
		
		//GraphDatabaseService graphDb;
	    //graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase( testDirectory.directory() );
	
	  //Starting an embedded database with configuration settings
		String pathdb = "/var/lib/neo4j/data/databases/graphDb";
	    graphDb = new GraphDatabaseFactory()
	.newEmbeddedDatabaseBuilder(new File(pathdb))
	.loadPropertiesFromFile( "/etc/neo4j/"+"neo4j.conf" )
	.newGraphDatabase();
	
	//}
	/*GraphDatabaseService database ;
	    GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database); //where database is an instance of GraphDatabaseService
	    runtime.registerModule(new UuidModule("UUID", UuidConfiguration.defaultConfiguration(), database));

	    configuration = ElasticSearchConfiguration.defaultConfiguration(HOST, PORT);
	    runtime.registerModule(new ElasticSearchModule("ES", new ElasticSearchWriter(configuration), configuration));

	    runtime.start();*/
  
    
    
     
    
   
	
	// match all relationships
   try (Transaction tx = graphDb.beginTx()) {
        Result result = graphDb.execute("CALL ga.es.queryRelationshipRaw('{\"query\":{\"match_all\":{},\\\"size\\\":100}') YIELD json return json");
        //ResourceIterator<Relationship> resIterator = result.columnAs("relationship");
        ResourceIterator<String> resIterator = result.columnAs("json");
        System.out.println("Result of the method " + resIterator.stream().count());
        //assertEquals(3, resIterator.stream().count());

        tx.success();
    }
	
    
   
   
    
	
   
   /*public void destroyTestDatabase()
   {*/
       graphDb.shutdown();
   //}
	
	
	
	
	

}
	
	/* private static Date getStartTimeFromManagementBean(
	            GraphDatabaseService graphDbService )
	    {
	        ObjectName objectName = JmxUtils.getObjectName( graphDbService, "Kernel" );
	        Date date = JmxUtils.getAttribute( objectName, "KernelStartTime" );
	        return date;
	    }
 */
}
