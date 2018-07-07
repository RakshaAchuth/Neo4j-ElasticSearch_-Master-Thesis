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



public class testswithelasticsearchquery12 {
	
	
	    public static void main(String[] args) throws IOException {

	    	Settings settings = Settings.builder()
	    	        .put("client.transport.ping_timeout", "300s").build();
	        TransportClient client = new PreBuiltTransportClient(settings)
	        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
	        
	        
	        long startTime= System.nanoTime();
	        
	        //get list of message ids that match the criteria (creationDate>20110721220000000)
	        String [] parameters2 = {"id","creationDate"};
	        BoolQueryBuilder matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "Message"));
	        matchingMessages.must(QueryBuilders.rangeQuery("creationDate").gt(20110721220000000L));
	        //matchingMessages.must(QueryBuilders.termQuery("id", 1511828861947L));
	       
	       
	       SearchResponse response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingMessages) 
	        		.setFetchSource(parameters2, null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        
	        Map<String, String> idListOfMatchingMessages2 = new HashMap<>();
	        //Set<String> idListOfMatchingMessages2 = new HashSet<>();
	        do {
	       
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingMessages2.put(it.getSource().get("id").toString(), it.getSource().get("creationDate").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("first.."+ idListOfMatchingMessages2.size()); 
	        
	        //terms query and agg on likes ,agg field should be TGTID and get the like count
	        List<?> clist = new ArrayList<>(idListOfMatchingMessages2.keySet());
	        int step = 100;
	        int endPos = idListOfMatchingMessages2.size()<step?idListOfMatchingMessages2.size():step;
	        int startPos = 0;
	        Map<Long, Long> map3 = new HashMap<>();
	    	do {
	        matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "LIKES") );
	        matchingMessages.must(QueryBuilders.termsQuery("TGTID", clist.subList(startPos, endPos)));
	        //matchingMessages.must(QueryBuilders.rangeQuery("likecount").gt(400));
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
	    	        //
	    	        do {
	    	        terms.forEach(it->{
	    	        	map3.put((Long)it.getKey(), it.getDocCount());
	    	        	//map3.put((String)it.getKey(), (int) it.getDocCount());
	    	        });
	    	        
	    	        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	    
	    	        }  
	    	        while(response.getHits().getHits().length != 0);
	    		        startPos = endPos+1;
	    		        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	    		        } while (startPos<clist.size());
	    	        
      	    	System.out.println("Second.."+ map3.size()); 
      	    	
//      	    	if(map3.size()>400)
//      	    	{
//      	    		System.out.println("like count" + map3);
//      	    	}
//	        map3.entrySet().forEach(entry->{
//	        	System.out.println(entry.getKey()+" :"
//	        			+ " "+entry.getValue());
//	        });
	
	        
	        /*get list of matching message ids along with the matching creationDate = 20110721220000000,  display the message id and 
	    	        creationdate based on the targetid from likes */ 
	      //here compare the msgid with src id of hascreator and get the tagtid from has creator
	    	        
	    	        
	    	//terms query on hascreator and store msgid and personid   
	        clist = new ArrayList<>(idListOfMatchingMessages2.keySet());
	        step = 100;
	        endPos = idListOfMatchingMessages2.size()<step?idListOfMatchingMessages2.size():step;
	        startPos = 0;
	        Map<Long, Long> map4 = new HashMap<>();
	    	do {
	        matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "HAS_CREATOR") );
	        matchingMessages.must(QueryBuilders.termsQuery("SRCID", clist.subList(startPos, endPos)));
	                
	        
	        response = client.prepareSearch()
		        		.setIndices("neo4j-index-final-relationship")
		        		.setQuery(matchingMessages)
		        		.setScroll(new TimeValue(60000))
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
	    	
	    	 System.out.println("Third.."+ map4.size());
//	    	 map4.entrySet().forEach(entry->{
//		        	System.out.println(entry.getKey()+" :"
//		        			+ " "+entry.getValue());
//		        });
	    
	        //compare the tgt id from hascreator and compare it with person id and display matching firstname and lastname
            startPos = 0;
	        step = 100;
	        endPos = map4.size()<step?map4.size():step;
            Map<Long, String> persondetails = new HashMap<>();
	        clist = new ArrayList<>(map4.values());
	        String [] parameters = {"firstName","lastName", "id"};
	        do {
	            BoolQueryBuilder matchingPeople = new BoolQueryBuilder();
	        	matchingPeople = new BoolQueryBuilder();
	        	matchingPeople.must(QueryBuilders.termQuery("_type", "Person"));
	        	matchingPeople.must(QueryBuilders.termsQuery("id", clist.subList(startPos, endPos)));
	        
	         
	               response = client.prepareSearch()
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingPeople) 
	        		.setFetchSource(parameters, null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000)
	        		.get();
	          
  	    do {
  	    	response.getHits().iterator().forEachRemaining(it->{
	        	persondetails.put(Long.parseLong(it.getSource().get("id").toString()), it.getSource().get("firstName")+","+it.getSource().get("lastName"));
	        });
  	    	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        
	    }
        while(response.getHits().getHits().length != 0);
        startPos = endPos+1;
        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
        } while (startPos<clist.size());
	        
	        System.out.println("Fourth.." + persondetails.size());
	        
//	        persondetails.entrySet().forEach(entry->{
//	        	System.out.println(entry.getKey()+" :"
//	        			+ " "+"persondetails.."+ entry.getValue());
//	        });
	        
	        //sort in descending order
	        List<Entry<Long, Long>> list = new ArrayList<>(map3.entrySet());
	        list.sort(Entry.comparingByKey());
	        Collections.sort(list, 
	                new Comparator<Entry<Long,Long>>() {
	                    @Override
	                    public int compare(Entry<Long,Long> e1, Entry<Long,Long> e2) {
	                        return e2.getValue().compareTo(e1.getValue());
	                    }
	                }
	        );
	        long elapsedTime= System.nanoTime()-startTime;
	        
	        list.forEach(it->{
	        	//System.out.println(it.getKey()+","+idListOfMatchingMessages2.get(it.getValue())+","+persondetails.get(map4.get(it.getValue())));
	        	System.out.println(it.getKey()+","+idListOfMatchingMessages2.get(it.getKey().toString())+","+persondetails.get(map4.get(it.getKey()))+","+it.getValue());
	        	//System.out.println(it.getKey()+","+idListOfMatchingMessages2.get(it.getKey())+","+idListOfMatchingMessages2.get(it.getValue())+","+persondetails.get(map4.get(it.getKey().toString()))+","+persondetails.get(map4.get(it.getValue()))+","+it.getValue());
	        	//System.out.println(it.getKey()+","+idListOfMatchingMessages2.get(it.getKey().toString())+","+persondetails.get(it.getKey().toString())+","+it.getValue());
	        });

	        System.out.println("Time taken (in nanoseconds): "+elapsedTime);
	        
	        //Close Transport Client Connection
	        client.close();
	    }
	}
	




    
