package mmdb.kvs.app.benchmarking;

import java.util.ArrayList;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.index.query.QueryBuilders.*;
import static java.util.Collections.reverseOrder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
//import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.ImmutableSettings;
//import org.elasticsearch.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filters.Filters.Bucket;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;
import org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters.getDegreeOptimizer;

import io.searchbox.core.search.aggregation.TermsAggregation;



public class testswithelasticsearchquery20 {

	  public static void main(String[] args) throws IOException {

	    	Settings settings = Settings.builder()
	    	        .put("client.transport.ping_timeout", "300s").build();
	        TransportClient client = new PreBuiltTransportClient(settings)
	        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
	        
	        
	        long startTime= System.nanoTime();
	        
	        
	        //String [] parameters2 = {"id","creationDate"};
	        BoolQueryBuilder matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "Message"));
	        //matchingMessages.must(QueryBuilders.rangeQuery("creationDate").gt(20110721220000000L));
	        //matchingMessages.must(QueryBuilders.termQuery("id", 1511828861947L));
	       
	       
	       SearchResponse response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingMessages) 
	        		.setFetchSource("id", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        
	       // Map<String, String> idListOfMatchingMessages = new HashMap<>();
	        Set<String> idListOfMatchingMessages = new HashSet<>();
	        do {
	       
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingMessages.add(it.getSource().get("id").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("first.."+ idListOfMatchingMessages.size()); 
	        
//	        //find all the tags
	        List<?> clist = new ArrayList<>(idListOfMatchingMessages);
	       int step = 100;
	       int endPos = idListOfMatchingMessages.size()<step?idListOfMatchingMessages.size():step;
	       int startPos = 0;
	        Map<Long, Long> map1 = new HashMap<>();
	    	do {
	        matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "HAS_TAG") );
	        matchingMessages.must(QueryBuilders.termsQuery("SRCID", clist.subList(startPos, endPos)));
	                
	        
	        response = client.prepareSearch()
		        		.setIndices("neo4j-index-final-relationship")
		        		.setQuery(matchingMessages)
		        		//.setFetchSource("TGTID", null)
		        		.setScroll(new TimeValue(60000))
		        		//.addAggregation(AggregationBuilders.terms("agg").field("TGTID").size(100000000))
		        		.setSize(10000)
		        		.get();
	                do {
	                	response.getHits().iterator().forEachRemaining(it->{
	        	        	map1.put(Long.parseLong(it.getSource().get("SRCID").toString()), Long.parseLong(it.getSource().get("TGTID").toString()));
	        	        });     
	    	        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	    	        }  
	    	        while(response.getHits().getHits().length != 0);
	    		        startPos = endPos+1;
	    		        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	    		        } while (startPos<clist.size());
	    	
	    	 System.out.println("hastag ID Count.."+ map1.size());
	        
	    	 map1.entrySet().forEach(entry->{
		        	System.out.println(entry.getKey()+" :"
		        			+ " "+entry.getValue());
		        });
	        
	    	 
	    	 
//	    	 //get message count
//	    	 
//	    	  BoolQueryBuilder matchingPeople = new BoolQueryBuilder();
//		        matchingPeople.must(QueryBuilders.termQuery("_type", "HAS_TAG"));
//		        matchingPeople.must(QueryBuilders.termsQuery("SRCID", idListOfMatchingMessages));
//		        //Get personsCount here
//		       response = client.prepareSearch()
//		        		
//		        		.setIndices("neo4j-index-final-relationship")
//		        		.setQuery(matchingPeople)
//		        		.setSize(0)
//	                    //.setFrom(i * scrollSize)
//		        		.addAggregation(AggregationBuilders.terms("agg").field("TGTID").size(100000000))
//		        		.get();
//		       System.out.println("Number of results: "+response.getHits().getTotalHits());
//		        Terms a = response.getAggregations().get("agg");
//		        List<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> terms= a.getBuckets();
//		        //
//		        Map<Long, Long> map1 = new HashMap<>();
//		        terms.forEach(it->{
//		        	map1.put((Long)it.getKey(), it.getDocCount());
//		        });
//		        System.out.println("Finished second part...");
////		        startPos = endPos+1;
////		        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
////		        } while (startPos<clist.size());
//		    	
//		    	System.out.println("third..."+ map1.size()); 
//		        map1.entrySet().forEach(entry->{
//		        	System.out.println(entry.getKey()+" :"
//		        			+ " "+entry.getValue());
//		        });
		        
	        
	    	//get matching tags
	        startPos = 0;
	        step = 100;
	        endPos =  map1.size()<step?map1.size():step;
	        //Map<Long, String> tags = new HashMap<>();
	         Set<String> idlistoftags = new HashSet<>();
	         clist = new ArrayList<>(map1.values());
	        //String [] parameters3 = {"id","name"};
	        BoolQueryBuilder matchingTag = new BoolQueryBuilder();
	        matchingTag.must(QueryBuilders.termQuery("_type", "Tag"));
	        matchingTag.must(QueryBuilders.termsQuery("id", clist.subList(startPos, endPos)));
	        
	      response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingTag) 
	        		.setFetchSource("id", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        
	        
	        do {
	       
	        response.getHits().iterator().forEachRemaining(it->{
	        	idlistoftags.add(it.getSource().get("id").toString());
	        	//tags.put(Long.parseLong(it.getSource().get("id").toString()), (String) it.getSource().get("name"));
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("list of tags.."+ idlistoftags.size());

	        
	        clist = new ArrayList<>(idlistoftags);
	        step = 100;
	        endPos = idlistoftags.size()<step?idlistoftags.size():step;
	        startPos = 0;
	        Map<Long, Long> map5 = new HashMap<>();
	    	do {
	    		matchingTag = new BoolQueryBuilder();
	    		matchingTag.must(QueryBuilders.termQuery("_type", "IS_SUBCLASS_OF") );
	    		matchingTag.must(QueryBuilders.termsQuery("SRCID", clist.subList(startPos, endPos)));
	                
	        
	        response = client.prepareSearch()
		        		.setIndices("neo4j-index-final-relationship")
		        		.setQuery(matchingTag)
		        		//.setFetchSource("TGTID", null)
		        		.setScroll(new TimeValue(60000))
		        		//.addAggregation(AggregationBuilders.terms("agg").field("TGTID").size(100000000))
		        		.setSize(10000)
		        		.get();
	                do {
	                	response.getHits().iterator().forEachRemaining(it->{
	        	        	map5.put(Long.parseLong(it.getSource().get("SRCID").toString()), Long.parseLong(it.getSource().get("TGTID").toString()));
	        	        });     
	    	        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	    	        }  
	    	        while(response.getHits().getHits().length != 0);
	    		        startPos = endPos+1;
	    		        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	    		        } while (startPos<clist.size());
	    	
	    	 System.out.println("issubclassof TargetID Count.."+ map5.size());
	        
	    	 map5.entrySet().forEach(entry->{
		        	System.out.println(entry.getKey()+" :"
		        			+ " "+entry.getValue());
		        });
	    	 
	    	 
	    	 
	    	 //Set<String> tagclassname = new HashSet<>();
		    clist = new ArrayList<>(map5.values());
		    step = 100;
		    endPos = idlistoftags.size()<step?idlistoftags.size():step;
		    startPos = 0;
	        String [] parameters = {"id","name"};
	        BoolQueryBuilder matchingtagclasses = new BoolQueryBuilder();
	        matchingtagclasses.must(QueryBuilders.termQuery("_type", "TagClass"));
	        matchingtagclasses.must(QueryBuilders.termQuery("name", "Single"));
	        matchingtagclasses.must(QueryBuilders.termQuery("name", "Country"));
	        matchingtagclasses.must(QueryBuilders.termQuery("name", "Writer"));
	        matchingtagclasses.must(QueryBuilders.termQuery("id", clist.subList(startPos, endPos)));
	        //matchingMessages.must(QueryBuilders.termQuery("id", 1511828861947L));
	       
	       
	    response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingMessages) 
	        		.setFetchSource(parameters, null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        
	        Map<String, String> idListOfMatchingtagclass= new HashMap<>();
	        //Set<String> idListOfMatchingMessages2 = new HashSet<>();
	        do {
	       
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingtagclass.put(it.getSource().get("id").toString(), it.getSource().get("name").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("matching tagclass.."+ idListOfMatchingtagclass.size()); 
	        
	        
	        List<Map.Entry<Long, Long>> list = map5.entrySet().stream().sorted(reverseOrder(Map.Entry.comparingByValue()))
	                .collect(Collectors.toList());
	        
	        list.forEach(it->{
	        	
	        	System.out.println(it.getKey()+","+idListOfMatchingtagclass.get(map5.get(it.getValue()))+","+it.getValue());
	        	
	        });
	        

    
    long elapsedTime= System.nanoTime()-startTime;
    

    System.out.println("Time taken (in nanoseconds): "+elapsedTime);
    
    //Close Transport Client Connection
    client.close();
}
}
	  
	  
