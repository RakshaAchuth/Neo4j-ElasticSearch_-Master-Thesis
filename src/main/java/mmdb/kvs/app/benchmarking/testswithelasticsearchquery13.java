package mmdb.kvs.app.benchmarking;


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



public class testswithelasticsearchquery13 {
	
	
	    public static void main(String[] args) throws IOException {

	    	Settings settings = Settings.builder()
	    	        .put("client.transport.ping_timeout", "300s").build();
	        TransportClient client = new PreBuiltTransportClient(settings)
	        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
	        
	        
	        long startTime= System.nanoTime();
	        
	        //get list of country names
	        String [] parameters2 = {"id","name"};
	        BoolQueryBuilder matchingCountry = new BoolQueryBuilder();
	        matchingCountry.must(QueryBuilders.termQuery("_type", "Country"));
	        //matchingCountry.must(QueryBuilders.termQuery("name", "Burma"));
	       
	       
	       SearchResponse response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingCountry) 
	        		.setFetchSource(parameters2, null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        
	        Map<String, String> idListOfMatchingCountry = new HashMap<>();
	        //Set<String> idListOfMatchingCountry = new HashSet<>();
	       
	        do {
	       
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingCountry.put(it.getSource().get("id").toString(),it.getSource().get("name").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("First.."+ idListOfMatchingCountry.size()); 
	        
	        idListOfMatchingCountry.entrySet().forEach(entry->{
	        	System.out.println(entry.getKey()+" :"
	        			+ " "+entry.getValue());
	        });
	        
	        //get list of matching messages with year and month
	        String [] parameters = {"id","creationDate"};
	        BoolQueryBuilder matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "Message"));
	        //get year
	        matchingMessages.must(QueryBuilders.scriptQuery(new Script("doc['creationDate'].value/10000000000000L")));
	        //get month
	        matchingMessages.must(QueryBuilders.scriptQuery(new Script("doc['creationDate'].value/100000000000L%100")));
	        
	          response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-test-node")   	
	        		.setQuery(matchingMessages) 
	        		.setFetchSource(parameters, null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	   
	        Map<String, String> idListOfMatchingMessages = new HashMap<>();
	          //Set<String> idListOfMatchingMessages = new HashSet<>();

	        do {
	 
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingMessages.put(it.getSource().get("id").toString(),it.getSource().get("creationDate").toString());
	        	//idListOfMatchingMessages.add(it.getSource().get("id").toString());

	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("Second.."+ idListOfMatchingMessages.size()); 
//	        idListOfMatchingMessages.entrySet().forEach(entry->{
//	        	System.out.println(entry.getKey()+" :"
//	        			+ " "+entry.getValue());
//	        });

	        
	        
//	       int startPos = 0;
//	       int step = 100;
//	       int endPos = step;
	       /* Map<Long, Long> map1 = new HashMap<>();
//	        List<?> clist = new ArrayList<>(idListOfMatchingMessages.keySet());
//	        do {
	        	matchingMessages = new BoolQueryBuilder();
	        	matchingMessages.must(QueryBuilders.termQuery("_type", "IS_LOCATED_IN"));
	        	matchingMessages.must(QueryBuilders.termsQuery("SRCID", idListOfMatchingMessages));
	        
	        response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-test-relationship")   	
	        		.setQuery(matchingTag) 
	        		.setFetchSource("TGTID", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	   
	        do {
	 
	        	response.getHits().iterator().forEachRemaining(it->{
    	        	map1.put(Long.parseLong(it.getSource().get("SRCID").toString()), Long.parseLong(it.getSource().get("TGTID").toString()));
    	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0);
//	        startPos = endPos+1;
//	        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
//	        //System.out.println("Looping..."+endPos);
//	        } while (startPos<clist.size());
	        System.out.println("done..." + map1.size());
	        map1.entrySet().forEach(entry->{
	        	System.out.println(entry.getKey()+" :"
	        			+ " "+entry.getValue());
	        }); */
	        
	           List<?> clist = new ArrayList<>(idListOfMatchingMessages.keySet());
	        int step = 100;
	        int endPos = step;
	        int startPos = 0;
	        Map<Long, Long> map3 = new HashMap<>();
	    	do {
	        matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "IS_LOCATED_IN") );
	        matchingMessages.must(QueryBuilders.termsQuery("SRCID", clist.subList(startPos, endPos)));
	                response = client.prepareSearch()
		        		
		        		.setIndices("neo4j-index-final-relationship")
		        		.setQuery(matchingMessages)
		        		.setSize(0)
		        		.setScroll(new TimeValue(60000))
		        		.addAggregation(AggregationBuilders.terms("agg").field("TGTID").size(100000000))
		        		.get();
	                //System.out.println("second 1..");
	                Terms b = response.getAggregations().get("agg");
	    	        List<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> terms= b.getBuckets();
	    	        
	    	        do {
	    	        terms.forEach(it->{
	    	        	map3.put((Long)it.getKey(), it.getDocCount());
	    	        });
	    	        
	    	        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	    	        
	    	    
	    	        }  
	    	        while(response.getHits().getHits().length != 0);
	    		        startPos = endPos+1;
	    		        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	    		        } while (startPos<clist.size());
	  
	        System.out.println("IsLocatedIn TargetID Count.."+ map3.size());
	        
	        map3.entrySet().forEach(entry->{
	        	System.out.println(entry.getKey()+" :"
	        			+ " "+entry.getValue());
	        }); 
	        
	        
	        //find all the tags
	        clist = new ArrayList<>(idListOfMatchingMessages.keySet());
	        step = 100;
	        endPos = idListOfMatchingMessages.size()<step?idListOfMatchingMessages.size():step;
	        startPos = 0;
	        Map<Long, Long> map4 = new HashMap<>();
	    	do {
	        matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "HAS_TAG") );
	        matchingMessages.must(QueryBuilders.termsQuery("SRCID", clist.subList(startPos, endPos)));
	                
	        
	        response = client.prepareSearch()
		        		.setIndices("neo4j-index-final-relationship")
		        		.setQuery(matchingMessages)
		        		.setFetchSource("TGTID", null)
		        		.setScroll(new TimeValue(60000))
		        		//.addAggregation(AggregationBuilders.terms("agg").field("TGTID").size(100000000))
		        		.setSize(10000)
		        		.get();
	                do {
	                	response.getHits().iterator().forEachRemaining(it->{
	        	        	map4.put(Long.parseLong(it.getSource().get("SRCID").toString()), Long.parseLong(it.getSource().get("TGTID").toString()));
	        	        });     
	    	        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	    	        }  
	    	        while(response.getHits().getHits().length != 0);
	    		        startPos = endPos+1;
	    		        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	    		        } while (startPos<clist.size());
	    	
	    	 System.out.println("IsReplyOf TargetID Count.."+ map4.size());
	        
	    	 map4.entrySet().forEach(entry->{
		        	System.out.println(entry.getKey()+" :"
		        			+ " "+entry.getValue());
		        });
	    	 
	        //get matching tags
	        startPos = 0;
	        step = 100;
	        endPos = step;
            Map<Long, String> tags = new HashMap<>();
	        clist = new ArrayList<>(map4.values());
	        String [] parameters3 = {"id","name"};
	        BoolQueryBuilder matchingTag = new BoolQueryBuilder();
	        matchingTag.must(QueryBuilders.termQuery("_type", "Tag"));
	        
	        response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingTag) 
	        		//.addAggregation(AggregationBuilders.terms("agg").field("id").size(100000000))
	    	        //.subAggregation(AggregationBuilders.terms("names").field("name").subAggregation(map4));
	    	     
	        		.setFetchSource(parameters3, null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        
	        //Map<String, String> idListOfMatchingCountry = new HashMap<>();
	        
	        //System.out.println("Number of results: "+response2.getHits().getTotalHits());
	        //Terms a = response.getAggregations().get("agg");
	        //List<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> terms= a.getBuckets();
	        do {
	       
	        response.getHits().iterator().forEachRemaining(it->{
	        	//tags.add(it.getSource().get("id").toString());
	        	tags.put(Long.parseLong(it.getSource().get("id").toString()), (String) it.getSource().get("name"));
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("list f tags.."+ tags.size());
//	        idListOfMatchingTag.entrySet().forEach(entry->{
//	        	System.out.println(entry.getKey()+" :"
//	        			+ " "+entry.getValue());
//	        });
	        
	     
	        List<Map.Entry<Long, Long>> list = map4.entrySet().stream().sorted(reverseOrder(Map.Entry.comparingByValue()))
	                .collect(Collectors.toList());
	        
	        list.forEach(it->{
	        	
	        	System.out.println(it.getKey()+","+tags.get(map4.get(it.getKey())));
	        	
	        });
	        
	        long elapsedTime= System.nanoTime()-startTime;
	        

	        System.out.println("Time taken (in nanoseconds): "+elapsedTime);
	        
	        //Close Transport Client Connection
	        client.close();
	    }
	}
	




    
