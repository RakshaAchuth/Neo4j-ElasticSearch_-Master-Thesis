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
import java.util.Set;
import java.util.Map.Entry;
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

import io.searchbox.core.search.aggregation.TermsAggregation;



public class testswithelasticsearchquery14 {
	
	
	    public static void main(String[] args) throws IOException {

	    	Settings settings = Settings.builder()
	    	        .put("client.transport.ping_timeout", "300s").build();
	        TransportClient client = new PreBuiltTransportClient(settings)
	        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
	        
	        
	        long startTime= System.nanoTime();
	        
	        //get list of post ids that match the criteria (creationDate>20110721220000000 and <20120630220000000)
	        BoolQueryBuilder matchingPosts = new BoolQueryBuilder();
	        matchingPosts.must(QueryBuilders.termQuery("_type", "Post"));
	        matchingPosts.must(QueryBuilders.rangeQuery("creationDate").gte(20120531220000000L));
	        matchingPosts.must(QueryBuilders.rangeQuery("creationDate").lte(20120630220000000L));
	        
	         
	       SearchResponse response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingPosts) 
	        		.setFetchSource("id", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        
	        Set<String> idListOfMatchingPosts = new HashSet<>();
	        //Map<String, String> idListOfMatchingPosts = new HashMap<>();
	        do {
	 
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingPosts.add(it.getSource().get("id").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("Matching Posts Count..(total post count)"+ idListOfMatchingPosts.size());
	        
	       //here we get the threadcount from has creator

	        BoolQueryBuilder matchingPeople = new BoolQueryBuilder();
	        matchingPeople.must(QueryBuilders.termQuery("_type", "HAS_CREATOR"));
	        matchingPeople.must(QueryBuilders.termsQuery("SRCID", idListOfMatchingPosts));
	        //Get personsCount here
	         SearchResponse   response2 = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-relationship")
	        		.setQuery(matchingPeople)
	        		.setSize(0)
                    //.setFrom(i * scrollSize)
	        		.addAggregation(AggregationBuilders.terms("agg").field("TGTID").size(100000000))
	        		.get();
	       System.out.println("Number of results: "+response2.getHits().getTotalHits());
	        Terms a = response2.getAggregations().get("agg");
	        List<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> terms= a.getBuckets();
	        //
	        Map<Long, Long> map1 = new HashMap<>();
	        terms.forEach(it->{
	        	map1.put((Long)it.getKey(), it.getDocCount());
	        });
	        System.out.println("Finished second part...");
//	        startPos = endPos+1;
//	        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
//	        } while (startPos<clist.size());
	    	
	    	System.out.println("third..."+ map1.size()); 
	        map1.entrySet().forEach(entry->{
	        	System.out.println(entry.getKey()+" :"
	        			+ " "+entry.getValue());
	        });
	        
	        
	        
	        
	      //get list of message ids that match the criteria (creationDate>20110721220000000 and < 20120630220000000)
	        BoolQueryBuilder matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "Message"));
	        matchingMessages.must(QueryBuilders.rangeQuery("creationDate").gte(20120531220000000L));
	        matchingMessages.must(QueryBuilders.rangeQuery("creationDate").lte(20120630220000000L));
	        
	         
	     response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingMessages) 
	        		.setFetchSource("id", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        
	        Set<String> idListOfMatchingMessages = new HashSet<>();
	        //Map<String, String> idListOfMatchingMessages = new HashMap<>();
	        do {
	 
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingMessages.add(it.getSource().get("id").toString());
	        	//idListOfMatchingMessages.put(it.getSource().get("id").toString(), it.getSource().get("creationDate").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("Matching Message Count.."+ idListOfMatchingMessages.size());

	        Map<Long, Long> msgCount = new HashMap<>();
	        idListOfMatchingPosts.forEach(post->{
	        	msgCount.put(Long.parseLong(post), 0L);
	        });
	        
	        int numHops = 0;
	        Set<String> nextHop = new HashSet<>(idListOfMatchingPosts);
	        String [] parameters = {"SRCID", "TGTID"};
	        Map<Long, Long> intermediateMap = new HashMap<>();
	        do{
	        	numHops++;
	        	System.out.println("On hop... "+numHops);
	        	matchingPosts = new BoolQueryBuilder();
	        	matchingPosts.must(QueryBuilders.termQuery("_type", "REPLY_OF"));
	        	matchingPosts.must(QueryBuilders.termsQuery("TGTID", nextHop));
		        
		        response = client.prepareSearch()
		        		.setIndices("neo4j-index-final-relationship")
		                .setQuery(matchingPosts)
        		    	.addAggregation(AggregationBuilders.terms("agg").field("TGTID").size(100000000))
		                .setSize(0).get();
		        a = response.getAggregations().get("agg");
		        terms= a.getBuckets();
		        
		        if (intermediateMap.isEmpty()){
		        	terms.forEach(it->{
		        		msgCount.put((Long)it.getKey(), (Long)it.getDocCount());
		        	});
		        }
		        else{
		        	terms.forEach(it->{
		        		msgCount.put(intermediateMap.get((Long)it.getKey()), msgCount.get(intermediateMap.get((Long)it.getKey()))+(Long)it.getDocCount());
		        	});
		        }
		        nextHop.clear();
		        response = client.prepareSearch()
		        		.setIndices("neo4j-index-final-relationship")   	
		        		.setQuery(matchingPosts) 
		        		.setFetchSource(parameters, null)
		        		.setScroll(new TimeValue(60000))
		        		.setSize(10000).get();
		        do {
		 
		        response.getHits().iterator().forEachRemaining(it->{
		        	intermediateMap.put((Long)it.getSource().get("SRCID"), intermediateMap.containsKey((Long)it.getSource().get("TGTID"))?intermediateMap.get((Long)it.getSource().get("TGTID")):(Long)it.getSource().get("TGTID"));
		        	nextHop.add(it.getSource().get("SRCID").toString());
		        });
		        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
		        }
		        while(response.getHits().getHits().length != 0);
		        System.out.println("About to hop for... "+nextHop.size());
	        } 
	        while(!nextHop.isEmpty());
	        
//	        /*Here we hop through the posts and collect all ids, we get the ids of all the posts matching the creationdate*/
//	        int candidateSize = 0;
//	        int numHops = 0;
//	        do{
//	        	candidateSize = candidates.size();
//	        	matchingPosts = new BoolQueryBuilder();
//	        	matchingPosts.must(QueryBuilders.termQuery("_type", "REPLY_OF"));
//	        	matchingPosts.must(QueryBuilders.termsQuery("TGTID", idListOfMatchingPosts));
//		        
//		        response = client.prepareSearch()
//		        
//		        		.setIndices("neo4j-index-final-relationship")
//		                .setQuery(matchingPosts)
//        		        .setFetchSource("SRCID", null)
//		                .setScroll(new TimeValue(60000))
//		                .setSize(10000).get();
//		        System.out.println("Hopping.. ."+numHops); 
//		        System.out.println("Number of comparisons..."+idListOfMatchingPosts.size());
//		        idListOfMatchingPosts.clear();
//		        do {
//		       	    response.getHits().forEach(t->{
//		       	    	idListOfMatchingPosts.add(t.getSource().get("SRCID").toString());
//			        });
//			        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
//			    }
//			    while(response.getHits().getHits().length != 0);
//		        
//		        
//		        numHops++;
//		        candidates.addAll(idListOfMatchingPosts);
//	        } 
//	        while(candidates.size()!=candidateSize);
//	        
//	        System.out.println("Finished hopping");
//	        System.out.println("Number of comparisons..."+candidates.size());
//	        

	        
	        //get the targetids  of based on the list of matching message ids
	     /*   List<?> clist = new ArrayList<>(idListOfMatchingMessages);
	        int step = 100;
	        int endPos = step;
	        int startPos = 0;
	        Map<Long, Long> map3 = new HashMap<>();
	    	do {
	        matchingMessages = new BoolQueryBuilder();
	        matchingMessages.must(QueryBuilders.termQuery("_type", "REPLY_OF") );
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
	    	
	    	//get post count and message count
	    	long value = ((long)response.getHits().getTotalHits())-map3.size();
	    	 Map<Long, Long> map = new HashMap<>();//Postcount to Messagecount
		        map.put((long) 0, value);
		        map3.entrySet().forEach(it->{
		        	if (map.containsKey(it.getValue())){
		        		map.put(it.getValue(),map.get(it.getValue())+1L);
		        	}
		        	else{
		        		map.put(it.getValue(),1L);
		        	}
		        });
	        
	        map3.entrySet().forEach(entry->{
	        	System.out.println(entry.getKey()+" :"
	        			+ " "+entry.getValue());
	        });
	        
	        System.out.println("ReplyOf TargetID Count.."+ map3.size());
	        
	        map3.entrySet().forEach(entry->{
	        	System.out.println(entry.getKey()+" :"
	        			+ " "+entry.getValue());
	        }); */
	        
	      //get the srcids  of based on the list of matching post ids
//	        List<?> clist = new ArrayList<>(idListOfMatchingPosts);
//	        int step = 100;
//	        int endPos = step;
//	        int startPos = 0;
//	        Map<Long, Long> map4 = new HashMap<>();
//	    	do {
//	    		matchingPosts = new BoolQueryBuilder();
//	    		matchingPosts.must(QueryBuilders.termQuery("_type", "REPLY_OF") );
//	    		matchingPosts.must(QueryBuilders.termsQuery("TGTID", clist.subList(startPos, endPos)));
//	                response = client.prepareSearch()
//		        		
//		        		.setIndices("neo4j-index-final-relationship")
//		        		.setQuery(matchingPosts)
//		        		.setSize(0)
//		        		.setScroll(new TimeValue(60000))
//		        		.addAggregation(AggregationBuilders.terms("agg").field("SRCID").size(100000000))
//		        		.get();
//	                //System.out.println("second 1..");
//	                Terms C = response.getAggregations().get("agg");
//	    	        List<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> terms= C.getBuckets();
//	    	        //
//	    	        do {
//	    	        terms.forEach(it->{
//	    	        	map4.put((Long)it.getKey(), it.getDocCount());
//	    	        });
//	    	        
//	    	        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
//	    	        
//	    	    
//	    	        }  
//	    	        while(response.getHits().getHits().length != 0);
//	    		        startPos = endPos+1;
//	    		        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
//	    		        } while (startPos<clist.size());
//	    	        
//	        
//	        map4.entrySet().forEach(entry->{
//	        	System.out.println(entry.getKey()+" :"
//	        			+ " "+entry.getValue());
//	        });
//	        
//	        System.out.println("ReplyOf SourceID Count.."+ map4.size()); 
	        
	      //terms query on hascreator and store msgid and personid   
	        List<?> clist = new ArrayList<>(idListOfMatchingMessages);
	        int  step = 100;
	         int  endPos = step;
	        int startPos = 0;
	        Map<Long, Long> map5 = new HashMap<>();
	        Map<Long, Long> post2person = new HashMap<>();
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
	        	        	map5.put(Long.parseLong(it.getSource().get("TGTID").toString()), Long.parseLong(it.getSource().get("TGTID").toString()));
	        	        	post2person.put(Long.parseLong(it.getSource().get("SRCID").toString()), Long.parseLong(it.getSource().get("TGTID").toString()));
	        	        });     
	    	        response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	    	        }  
	    	        while(response.getHits().getHits().length != 0);
	    		        startPos = endPos+1;
	    		        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	    		        } while (startPos<clist.size());
	    	
	    	 System.out.println("Third.."+ map5.size());
//	    	 map5.entrySet().forEach(entry->{
//		        	System.out.println(entry.getKey()+" :"
//		        			+ " "+entry.getValue());
//		        });
		        
	    	 
	    	 
	    	 
	    	 //we search for ids from the previous step to search for people that created the messages (in Has_Creator) 
	       /* clist = new ArrayList<>(idListOfMatchingMessages);
	        int step = 100;
	        int endPos = idListOfMatchingMessages.size()<step?idListOfMatchingMessages.size():step;
	        int startPos = 0;
	        Map<Long, Long> map5 = new HashMap<>();
	    	do {
	        BoolQueryBuilder matchingPeople = new BoolQueryBuilder();
	        matchingPeople.must(QueryBuilders.termQuery("_type", "HAS_CREATOR"));
	        matchingPeople.must(QueryBuilders.termsQuery("SRCID", clist.subList(startPos, endPos)));
	        //Get personsCount here
	        SearchResponse response2 = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-relationship")
	        		.setQuery(matchingPeople)
	        		.setSize(0)
                    //.setFrom(i * scrollSize)
	        		.addAggregation(AggregationBuilders.terms("agg").field("TGTID").size(100000000))
	        		.get();
	       System.out.println("Number of results: "+response2.getHits().getTotalHits());
	        Terms a = response2.getAggregations().get("agg");
	        List<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> terms= a.getBuckets();
	        //
	       // Map<Long, Long> map5 = new HashMap<>();
	        terms.forEach(it->{
	        	map5.put((Long)it.getKey(), it.getDocCount());
	        });
	        
	        startPos = endPos+1;
	        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	        } while (startPos<clist.size());
	    	
	    	System.out.println("third..."+ map5.size()); */
//	        map5.entrySet().forEach(entry->{
//	        	System.out.println(entry.getKey()+" :"
//	        			+ " "+entry.getValue());
//	        });
	        
	        
	      //terms query on the personid and get the person firstname and lastname 
            startPos = 0;
	        step = 100;
	        endPos = step;
            Map<Long, String> persondetails = new HashMap<>();
	        clist = new ArrayList<>(map5.values());
	        String [] parameters1 = {"firstName","lastName", "id"};
	        do {
	        	BoolQueryBuilder Peopledetails = new BoolQueryBuilder();
	        	Peopledetails.must(QueryBuilders.termQuery("_type", "Person"));
	        	Peopledetails.must(QueryBuilders.termsQuery("id", clist.subList(startPos, endPos)));
	    
	        	//SearchResponse  response2 = client.prepareSearch()
	        	response = client.prepareSearch()
	        			
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(Peopledetails) 
	        		.setFetchSource(parameters1, null)
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
	        persondetails.entrySet().forEach(entry->{
	        	System.out.println(entry.getKey()+" :"
	        			+ " "+entry.getValue());
	        });
	        
	       // List<Entry<Long, String>> list = new ArrayList<>(persondetails.entrySet());
	        List<Map.Entry<Long, Long>> list = map5.entrySet().stream().sorted(reverseOrder(Map.Entry.comparingByValue()))
	                .collect(Collectors.toList());
	        Map<Long, Long> person2msgcount = new HashMap<>();
	        post2person.entrySet().forEach(post->{
	        	long value = 0L;
	        	if (person2msgcount.containsKey(post.getValue())){
	        		value+=person2msgcount.get(post.getValue());
	        	}
	        	if (msgCount.containsKey(post.getKey())){
		        	value+=msgCount.get(post.getKey());
		        }
	        	person2msgcount.put(post.getValue(), value);
	        });
	        
	        list.forEach(it->{
	        	//System.out.println(it.getKey()+","+idListOfMatchingMessages.get(it.getKey().toString())+","+persondetails.get(map5.get(it.getKey().toString()))+","+it.getValue());
	        	System.out.println(it.getKey()+","+persondetails.get(map5.get(it.getKey()))+","+(map1.get(it.getValue()))+", "+person2msgcount.get(it.getKey()));
	        	//System.out.println(it.getKey()+","+persondetails.get(map5.get(it.getKey()))+","+(it.getKey().toString())+","+(it.getKey().toString()));
	        });
	        
	        long elapsedTime= System.nanoTime()-startTime;
	        
	        System.out.println("Time taken (in nanoseconds): "+elapsedTime);
	        
	        //Close Transport Client Connection
	        client.close();
	    }
	}
	




    
