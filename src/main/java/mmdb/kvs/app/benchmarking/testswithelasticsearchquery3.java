package mmdb.kvs.app.benchmarking;


import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.get.GetResponse;
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



public class testswithelasticsearchquery3 {
	
	
	    public static void main(String[] args) throws IOException {

	    	Settings settings = Settings.builder()
	    	        .put("client.transport.ping_timeout", "300s").build();
	        TransportClient client = new PreBuiltTransportClient(settings)
	        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
	        
	        /* 1. Here we search for messages with creationdate as year and month matching the criteria*/
	        long startTime= System.nanoTime();
	        BoolQueryBuilder matchingTag = new BoolQueryBuilder();
	        matchingTag.must(QueryBuilders.termQuery("_type", "Message"));
	        matchingTag.must(QueryBuilders.scriptQuery(new Script("doc['creationDate'].value/10000000000000L == 2010")));
	        matchingTag.must(QueryBuilders.scriptQuery(new Script("doc['creationDate'].value/100000000000L%100 == 10")));
	        
	        SearchResponse response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-test-node")   	
	        		.setQuery(matchingTag) 
	        		.setFetchSource("id", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	   
	        Set<String> idListOfMatchingMessages1 = new HashSet<>();
	        do {
	 
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingMessages1.add(it.getSource().get("id").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	   
	        matchingTag = new BoolQueryBuilder();
	        matchingTag.must(QueryBuilders.termQuery("_type", "Message"));
	        matchingTag.must(QueryBuilders.scriptQuery(new Script("doc['creationDate'].value/10000000000000L == 2010")));
	        matchingTag.must(QueryBuilders.scriptQuery(new Script("doc['creationDate'].value/100000000000L%100 == 11")));
	        
	        response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-test-node")   	
	        		.setQuery(matchingTag) 
	        		.setFetchSource("id", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	   
	        Set<String> idListOfMatchingMessages2 = new HashSet<>();
	        do {
	 
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingMessages2.add(it.getSource().get("id").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0); 
	        System.out.println("We finished the first part...");
	        
	        int startPos = 0;
	        int step = 100;
	        int endPos = step;
	        Map<String, Integer> countMonth1 = new HashMap<>();
	        List<String> clist = new ArrayList<>(idListOfMatchingMessages1);
	        do {
	        matchingTag = new BoolQueryBuilder();
	        matchingTag.must(QueryBuilders.termQuery("_type", "HAS_TAG"));
	        matchingTag.must(QueryBuilders.termsQuery("SRCID", clist.subList(startPos, endPos)));
	        
	        response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-test-relationship")   	
	        		.setQuery(matchingTag) 
	        		.setFetchSource("TGTID", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	   
	        
	        do {
	 
	        response.getHits().iterator().forEachRemaining(it->{
	        	String temp = it.getSource().get("TGTID").toString();
	        	countMonth1.put(temp, countMonth1.containsKey(temp)?countMonth1.get(temp)+1:1);
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0);
	        startPos = endPos+1;
	        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	        } while (startPos<clist.size());
	        
	        System.out.println("We finished the second part...");
	        
	        startPos = 0;
	        step = 100;
	        endPos = step;
	        Map<String, Integer> countMonth2 = new HashMap<>();
	        clist = new ArrayList<>(idListOfMatchingMessages2);
	        do {
	        matchingTag = new BoolQueryBuilder();
	        matchingTag.must(QueryBuilders.termQuery("_type", "HAS_TAG"));
	        matchingTag.must(QueryBuilders.termsQuery("SRCID", clist.subList(startPos, endPos)));
	        
	        response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-test-relationship")   	
	        		.setQuery(matchingTag) 
	        		.setFetchSource("TGTID", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	   
	        do {
	 
	        response.getHits().iterator().forEachRemaining(it->{
	        	String temp = it.getSource().get("TGTID").toString();
	        	countMonth2.put(temp, countMonth2.containsKey(temp)?countMonth2.get(temp)+1:1);
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0);
	        startPos = endPos+1;
	        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	        //System.out.println("Looping..."+endPos);
	        } while (startPos<clist.size());
	        System.out.println("We finished the third part...");
	        
	        Map<String, Integer> diffs = new HashMap<>();
	        countMonth1.entrySet().forEach(it->{
	        	diffs.put(it.getKey(), countMonth2.containsKey(it.getKey())? Math.abs(it.getValue()-countMonth2.get(it.getKey())) :it.getValue());
	        });
	        countMonth2.entrySet().forEach(it->{
	        	if (!diffs.containsKey(it.getKey())){
	        		diffs.put(it.getKey(), it.getValue());	
	        	}	        	
	        });
	        Map<String, Integer> map = diffs.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).collect(Collectors.toMap(
	                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
	        
        
	        /*Here we search for the hasTag that have as source the message ids that we have, and from that we get all the tgtIds.*/
	        /*Then we store in a hash each targetId with the cound of how many times it appeared.*/
	        /*We do this for both months*/
	        /*Then we do a diff and we sort them descending, and get the top ids...*/
	        
	        Map<String, Integer> map2 = new LinkedHashMap<>();
	        map2 = map.entrySet().stream().limit(100).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
	        int count = 0;
	        int value  = 0;
	        for (Map.Entry<String, Integer>m:map.entrySet()){
	        	
	        	if (count == 99){
	        		value = m.getValue();
	        	}
	        	if(count > 99 && m.getValue()==value)
	        	{
	        		map2.put(m.getKey(), m.getValue());
	        	}
	        	count ++;
	        }
//	        map2.entrySet().forEach(it->{
//	        	System.out.println(it.getValue());
//	        });
	        //System.out.println(map2.size());
	        
	        startPos = 0;
	        step = 100;
	        endPos = step;
	        Map<String, String> tagname = new HashMap<>();
	        clist = new ArrayList<>(map2.keySet());
	        String [] parameters = {"name", "id"};
	        do {
	        matchingTag = new BoolQueryBuilder();
	        matchingTag.must(QueryBuilders.termQuery("_type", "Tag"));
	        matchingTag.must(QueryBuilders.termsQuery("id", clist.subList(startPos, endPos)));
	        
	        response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-test-node")   	
	        		.setQuery(matchingTag) 
	        		.setFetchSource(parameters, null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	   
	        do {
	 
	        response.getHits().iterator().forEachRemaining(it->{
	        	String temp = it.getSource().get("id").toString();
	        	tagname.put(temp, it.getSource().get("name").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        while(response.getHits().getHits().length != 0);
	        startPos = endPos+1;
	        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	        //System.out.println("Looping..."+endPos);
	        } while (startPos<clist.size());
	        
//	       
//	        Map<String, Integer> map5 = new HashMap<>();
//	        map5.put("name", value);
//	        map2.entrySet().forEach(it->{
//	        	if (map5.containsKey(it.getValue())){
//	        		map5.put(it.getKey(),(int) (map5.get(it.getValue())+1L));
//	        	}
//	        	else{
//	        		map5.put(it.getKey(),(int) 1L);
//	        	}
//	        });
//	        List<Entry<String, Integer>> list2 = map5.entrySet().stream().sorted(reverseOrder(Map.Entry.comparingByValue()))
//	                .collect(Collectors.toList());
//	       
	        
	        
	        // only sort missing map2 based on tag name and diff
	        long elapsedTime= System.nanoTime()-startTime;
	        
	        
//	        list2.forEach(it->{
//	        	System.out.println("tag name: "+it.getValue()+", diffs: "+it.getValue());
//	        });
//	        
	        
	        tagname.entrySet().forEach(it->{
	        	System.out.println("tag name: "+it.getValue());
	        });
	        
	        map2.entrySet().forEach(it->{
	        	System.out.println("diffs: "+it.getValue());
	        });
	        System.out.println("Time taken (in nanoseconds): "+elapsedTime);
	        
	        
	        
	        //Next query: All sourceIds of REPLY_OF, where tgtId is in idList.
            
	        //Close Transport Client Connection
	        client.close();
	    }
	}
	




    
