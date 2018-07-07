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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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



public class testswithelasticsearchquery18 {
	
	
	    public static void main(String[] args) throws IOException {

	        //Cluster Name - samplename
	        //Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true).put("cluster.name", "samplename").build();

	    	
	       /* Client client = TransportClient.builder.build()
	        		   .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9200));
	        */
	        
	    	Settings settings = Settings.builder()
	    	        .put("client.transport.ping_timeout", "300s").build();
	        TransportClient client = new PreBuiltTransportClient(settings)
	        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
	        
//	        RestClient restClient = RestClient.builder(new HttpHost("localhost", 900))
//	                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
//	                    @Override
//	                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
//	                        return requestConfigBuilder.setConnectTimeout(5000)
//	                                .setSocketTimeout(30000);
//	                    }
//	                })
//	                .setMaxRetryTimeoutMillis(30000)
//	                .build();

	        
	        //Sample Query
	       // String queryString = "{\"query\":{\"query_string\":{\"query\":\"field:value\"}},\"fields\": [\"Firefox\"]}";
	        
	        //String queryString = "{\"query\":{\"match\":{\"_type\":\"Comment\"}},\"size\":100}";
	        String queryString = "{\"size\": 10, \"_source\":[\"id\"],\"query\": { \"bool\": {\"should\": [{\"bool\": "
	        		+ "{\"must\": [{\"term\": { \"_type\": \"Post\"}},{\"term\": {\"langauge\": \"ar\"}}]}},{\"term\": "
	        		+ "{\"_type\": \"Comment\"}}]}}}";
	        
	        
	          

	        
	        
	        //String queryString = "{\"query\":{\"match\":{\"name\":\"Tom Tykwer\"}},"
	    			//+ "\"minimum_should_match\": \"1\"}";
	        

	        //Sample Query - JSONObject
	        // convert the raw query string to JSONObject to avoid query parser error in Elasticsearch
	       // JSONObject queryStringObject = new JSONObject(queryString);
	        
	        //queryString="LOL";
//	        QueryBuilder qb = QueryBuilders.queryStringQuery(queryString);
	        //String queryString = null;
	        //Match query
	        //QueryBuilder qb = QueryBuilders.matchQuery("id", queryString);
	       
	        
//	        "query": {
//	            "bool": {
//	              "must":[
//	              "should": [
//	                {
//	                  "bool": {
//	                    "must": [
//	                      {
//	                        "term": {
//	                          "_type": "Post"
//	                        }
//	                      },
//	                      {
//	                        "term": {
//	                          "langauge": "ar"
//	                        }
//	                      }
//	                    ]
//	                  }
//	                },
//	                {
//	                  "term": {
//	                    "_type": "Comment"
//	                  }
//	                }], { "range": { "creationDate": { "gt": 20110722000000000}}}, 
//	            {"range": { "length": { "lt": 20}}}
//	              ]
//	            }
//	          }
	        
	        //match all query
	        //QueryBuilder qb = QueryBuilders.matchAllQuery();
	        //int scrollSize = 20000;
	        //int i = 0;
	        /*Here we search for posts that have the language and match the criteria*/
	        long startTime= System.nanoTime();
	        BoolQueryBuilder matchingPosts = new BoolQueryBuilder();
	        matchingPosts.must(QueryBuilders.termQuery("_type", "Message"));
	        matchingPosts.must(QueryBuilders.termQuery("isComment", "false"));
	        matchingPosts.must(QueryBuilders.termQuery("language", "ar"));
	        matchingPosts.must(QueryBuilders.rangeQuery("creationDate").gt(20110722000000000L));
	        matchingPosts.must(QueryBuilders.rangeQuery("length").lte(20L));
	        
	        //qb.must(QueryBuilders.rangeQuery("creationDate").gt(20110722000000000L));
	        //qb.must(QueryBuilders.rangeQuery("length").lt(20L));
//	        QueryBuilder internalQuery = QueryBuilders.boolQuery().must();
	        
	        //QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().should()));
	        
	        /*String queryString;
	        //multi match query
	        QueryBuilder qb = QueryBuilders.multiMatchQuery(
	        	    "firefox",     
	        	    "browserUsed", queryString          
	        	    );
	        */
	        //haschild query
	        
	        SearchResponse response = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingPosts) 
	        		.setFetchSource("id", null)
	        		//.setSize(scrollSize)
                    //.setFrom(i * scrollSize)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        		//.execute().actionGet();
	        //GetResponse response = client.prepareGet("neo4j-index-test-node", "Post", "1").get();
	        Set<String> idListOfMatchingMessages = new HashSet<>();
	        do {
	 
	        //System.out.println("Number of results: "+response.getHits().getTotalHits());
	        response.getHits().iterator().forEachRemaining(it->{
	        	idListOfMatchingMessages.add(it.getSource().get("id").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        	while(response.getHits().getHits().length != 0); 
	        //System.out.println("Number of results: "+response.getHits().getTotalHits());
	        //System.out.println("Number of stored ids: "+idListOfMatchingMessages.size());


	        /*Here we search for posts that have the language only (so we will not include them in the ids list)*/
	        Set<String> idsNextHop = new HashSet<>();
	        Set<String> candidates = new HashSet<>();
	        //Now we get the ids of posts in the language AR (only that)
	        matchingPosts = new BoolQueryBuilder();
	        matchingPosts.must(QueryBuilders.termQuery("_type", "Message"));
	        matchingPosts.must(QueryBuilders.termQuery("isComment", "false"));
	        matchingPosts.must(QueryBuilders.termQuery("language", "ar"));
	        response = client.prepareSearch()
	        		.setIndices("neo4j-index-final-node")   	
	        		.setQuery(matchingPosts) 
	        		.setFetchSource("id", null)
	        		.setScroll(new TimeValue(60000))
	        		.setSize(10000).get();
	        do {
	        	response.getHits().iterator().forEachRemaining(it->{
	        	idsNextHop.add(it.getSource().get("id").toString());
	        });
	        	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        }
	        	while(response.getHits().getHits().length != 0);
	        
	         //System.out.println("Number of stored ids: "+idListOfMatchingMessages.size());
	        /*Here we hop through the messages and collect all ids*/
	        int candidateSize = 0;
	        int numHops = 0;
	        do{
	        	candidateSize = candidates.size();
	        	BoolQueryBuilder matchingMsgs = new BoolQueryBuilder();
		        matchingMsgs.must(QueryBuilders.termQuery("_type", "REPLY_OF"));
		        matchingMsgs.must(QueryBuilders.termsQuery("TGTID", idsNextHop));
		        SearchResponse response2 = client.prepareSearch()
		        .setIndices("neo4j-index-final-relationship")
		        .setQuery(matchingMsgs)
        		.setFetchSource("SRCID", null)
		        .setScroll(new TimeValue(60000))
		        .setSize(10000).get();
		        //System.out.println("Hopping for the best..."+numHops);
		        //System.out.println("Number of comparisons..."+idsNextHop.size());
		        idsNextHop.clear();
		        do {
		       	    response2.getHits().forEach(t->{
			        	idsNextHop.add(t.getSource().get("SRCID").toString());
			        });
			        response2 = client.prepareSearchScroll(response2.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
			    }
			    while(response2.getHits().getHits().length != 0);
		        
		        
		        numHops++;
		        candidates.addAll(idsNextHop);
	        } 
	        while(candidates.size()!=candidateSize);
	        
	        //System.out.println("We finished the hopping");
	        //System.out.println("Number of comparisons..."+candidates.size());
	        
	        List<?> clist = new ArrayList<>(candidates);
	        
	        /*Here we filter out the messages that do not match the criteria*/
	        int startPos = 0;
	        int step = 100;
	        int endPos = step;
	        do {
        	BoolQueryBuilder matchingMsgs = new BoolQueryBuilder();
	        matchingMsgs.must(QueryBuilders.termQuery("_type", "Message"));
	        matchingMsgs.must(QueryBuilders.termsQuery("id", clist.subList(startPos, endPos)));
	        matchingMsgs.must(QueryBuilders.rangeQuery("creationDate").gt(20110722000000000L));
	        matchingMsgs.must(QueryBuilders.rangeQuery("length").lte(20L));
	        matchingMsgs.must(QueryBuilders.existsQuery("content"));
	        
	        SearchResponse response2 = client.prepareSearch()
	        .setIndices("neo4j-index-final-node")
	        .setQuery(matchingMsgs)
    		.setFetchSource("id", null)
	        .setScroll(new TimeValue(60000))
	        .setSize(10000).get();
	        do {
	       	    response2.getHits().forEach(t->{
		        	idListOfMatchingMessages.add(t.getSource().get("id").toString());
		        });
		        response2 = client.prepareSearchScroll(response2.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
		    }
		    while(response2.getHits().getHits().length != 0);
	        startPos = endPos+1;
	        endPos=(endPos+step>clist.size())?clist.size():step+endPos;
	        //System.out.println("Looping..."+endPos);
	        } while (startPos<clist.size());
	        System.out.println("Finished filtering query");
	        //System.out.println("first.."+ idListOfMatchingMessages.size());
	        
	        BoolQueryBuilder matchingPeople = new BoolQueryBuilder();
	        matchingPeople.must(QueryBuilders.termQuery("_type", "HAS_CREATOR"));
	        matchingPeople.must(QueryBuilders.termsQuery("SRCID", idListOfMatchingMessages));
	        //System.out.println("Number of comparisons: "+idListOfMatchingMessages);
	        
	        //qb.must(QueryBuilders.rangeQuery("creationDate").gt(20110722000000000L));
	        //qb.must(QueryBuilders.rangeQuery("length").lt(20L));
	        //QueryBuilder internalQuery = QueryBuilders.boolQuery().must();
	        
	        //QueryBuilders.boolQuery() must(QueryBuilders.boolQuery().should()));
	        
	        /*String queryString;
	        //multi match query
	        QueryBuilder qb = QueryBuilders.multiMatchQuery(
	        	    "firefox",     
	        	    "browserUsed", queryString          
	        	    );
	        */
	        //haschild query
	        
	        /*Here we get the personsCount*/
	        SearchResponse response2 = client.prepareSearch()
	        		
	        		.setIndices("neo4j-index-final-relationship")
	        		.setQuery(matchingPeople)
	        		.setSize(0)
                    //.setFrom(i * scrollSize)
	        		.addAggregation(AggregationBuilders.terms("agg").field("TGTID").size(100000000))
	        		.get();
	        		//.execute().actionGet();
	        //GetResponse response = client.prepareGet("neo4j-index-test-node", "Post", "1").get();
	        //List<String> People = new ArrayList<>();
	        //do {
	 
	        //System.out.println("Number of results: "+response.getHits().getTotalHits());
	        Terms a = response2.getAggregations().get("agg");
	        List<org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket> terms= a.getBuckets();
	        //
	        Map<Long, Long> map = new HashMap<>();
	        terms.forEach(it->{
	        	map.put((Long)it.getKey(), it.getDocCount());
	        });
	        System.out.println("Finished second part...");
	        //response2.getHits().forEach(t->{
	        //	People.add(t.getSource().get("TGTID").toString());
	        //});
	        //response2 = client.prepareSearchScroll(response2.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	        //}
	        //while(response2.getHits().getHits().length != 0);
	        
	        
	        BoolQueryBuilder totalPeople = new BoolQueryBuilder();
	        totalPeople.must(QueryBuilders.termQuery("_type", "Person"));
	        
	        response2 = client.prepareSearch()
	        		.setIndices("neo4j-index-final-node")
	        		.setQuery(totalPeople)
	        		.setSize(0).get();
	        long value = ((long)response2.getHits().getTotalHits())-map.size(); 
	        
	        //System.out.println("Number of results: "+response2.getHits().getTotalHits());
	        //System.out.println("Number of results in people: "+People.size());
	        //List<Map.Entry<Long, Long>> list = new ArrayList<>(map.entrySet());
	        //list.sort(Map.Entry.comparingByValue());
	        Map<Long, Long> map2 = new HashMap<>();//Messagecount to personcount
	        map2.put((long) 0, value);
	        map.entrySet().forEach(it->{
	        	if (map2.containsKey(it.getValue())){
	        		map2.put(it.getValue(),map2.get(it.getValue())+1L);
	        	}
	        	else{
	        		map2.put(it.getValue(),1L);
	        	}
	        });
//	        map2.put(0, value);
//	        list.forEach(it->{
//	        	if (map2.containsKey(it.getValue())){
//	        		map2.put(it.getValue(), map2.get(it.getValue())+1);
//	        	}
//	        	else {
//	        		map2.put(it.getValue(), 1);
//	        	}
//	        });
	        
	        List<Map.Entry<Long, Long>> list2 = map2.entrySet().stream().sorted(reverseOrder(Map.Entry.comparingByValue()))
	                .collect(Collectors.toList());
	        
	        long elapsedTime= System.nanoTime()-startTime;
	        
	        
	        list2.forEach(it->{
	        	System.out.println("Message count"+it.getKey()+", Person count:"+it.getValue());
	        });
	        System.out.println("Time taken (in nanoseconds): "+elapsedTime);
	        
	        //Close Transport Client Connection
	        client.close();
	    }
	}
	 
