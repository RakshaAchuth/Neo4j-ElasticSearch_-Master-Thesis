//package mmdb.kvs.app.es;
//import java.io.IOException;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.Map;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.client.transport.TransportClient;
////import org.elasticsearch.common.settings.ImmutableSettings;
//import org.elasticsearch.*;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.index.query.MatchQueryBuilder;
//import org.elasticsearch.index.query.QueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.index.query.QueryStringQueryBuilder;
//import org.elasticsearch.search.SearchHitField;
//import org.elasticsearch.search.SearchHits;
//import org.json.JSONObject;
//
//public class TestsWithES {
//    public static void main(String[] args) throws IOException {
//
//        //Cluster Name - samplename
//        //Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true).put("cluster.name", "samplename").build();
//
//        //Specify the IP address of Elasticsearch Master Node
//        //Client esclient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9200));
//
//        
//        Client client = TransportClient.builder().build()
//        		   .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
//        
//        //Sample Query
//       // String queryString = "{\"query\":{\"query_string\":{\"query\":\"field:value\"}},\"fields\": [\"Firefox\"]}";
//        
//        String queryString = "{\"query\":{\"match\":{\"_type\":\"Comment\"}},\"size\":100}";
//        //String queryString = "{\"query\":{\"match\":{\"name\":\"Tom Tykwer\"}},"
//    			//+ "\"minimum_should_match\": \"1\"}";
//        
//
//        //Sample Query - JSONObject
//        // convert the raw query string to JSONObject to avoid query parser error in Elasticsearch
//        JSONObject queryStringObject = new JSONObject(queryString);
//        
//        queryString="LOL";
////        QueryBuilder qb = QueryBuilders.queryStringQuery(queryString);
//        
//        //Match query
//        //QueryBuilder qb = QueryBuilders.matchQuery("content", queryString);
//       
//        
//        //multi match query
//        QueryBuilder qb = QueryBuilders.multiMatchQuery(
//        	    "firefox",     
//        	    "browserUsed", queryString          
//        	    );
//        
//        //haschild query
//        
//        
//        //boosting query
//       /* QueryBuilder qb = QueryBuilders.boostingQuery()
//        .positive(QueryBuilders.termQuery("browserUsed",queryString))
//        .negative(QueryBuilders.termQuery("Content",queryString))
//        .negativeBoost(0.2f);*/
//        
//        //ID's query
//        //QueryBuilder qb = QueryBuilders.idsQuery().ids("1", "2");
//        
//        //constantScoreQuery
//        /*QueryBuilder qb = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("content",queryString))
//        .boost(2.0f);
//        */
//        
//        //fieldQuery
//        //QueryBuilder qb = QueryBuilders.fieldQuery("content", queryString);
//        //QueryBuilder qb = QueryBuilders.queryStringQuery("browserUsed").field("content");
//        
//        
//        //Elasticsearch Response
//        //SearchResponse response = client.prepareSearch("indexName").setTypes("typeName").setSource(queryStringObject.toString()).execute().actionGet();
//        SearchResponse response = client.prepareSearch("neo4j-index-node").setTypes("Comment").setQuery(qb).addField("browserUsed").addField("content").execute().actionGet();
//        //Elasticsearch Response Hits
//        SearchHits hits = response.getHits();
//
//        
//        //Iterate SearchHits Object to get the Documents
//        //i.e. For each document
//        for (int i = 0; i < hits.getHits().length; i++) {
//
//            //Fields Object for each document
//            Map<String, SearchHitField> responseFields = hits.getAt(i).getFields();
//            System.out.println("Hit "+i);
//            responseFields.keySet().forEach(it->{
//            	System.out.println(it+":"+responseFields.get(it).getValue().toString());
//            });
//            //Access required field
//           // SearchHitField field = responseFields.get("fieldname");
//            //SearchHitField field = responseFields.get("_type");  
//            
//            
//            
//            //Print field value
//            //field.getValue() return single value
//            //field.getValues()returns the value in an array
//            //System.out.println(field.getValue().toString());
//        }
//
//        //Close Transport Client Connection
//        client.close();
//    }
//}