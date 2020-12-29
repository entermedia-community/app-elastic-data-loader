package org.entermediadb;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        try
        {
	    		RestHighLevelClient client = new RestHighLevelClient(
	    		        RestClient.builder(
	    		                new HttpHost("154.127.54.122", 9270, "http")
	    		                //,    new HttpHost("localhost", 9201, "http"))
	    		             )
	    		 );
	    		//setMapping(client);
	    		//loadData(client);
	    		client.close();
		}
		catch (Throwable ex)
		{
			ex.printStackTrace();
		}
    }
    /*
    public void setMapping(RestHighLevelClient client, String indexname, String tablename) throws Exception
    {
    	//public void putMapping(MediaArchive mediaarchive, String searchtype, Page upload, String tempindex) throws Exception{
    		
    		AdminClient admin = client.admin();
    		PutMappingRequest req = Requests.putMappingRequest(indexname);//.type(tablename);
    		String uploadGetContent = upload.getContent()
    		req = req.source(uploadGetContent);

    		req.validate();
    		PutMappingResponse pres = admin.indices().putMapping(req).actionGet();



    //	}


    }
    */
    
    /*
    public void loadData(RestHighLevelClient client)
    {
    	//public void importJson(Data site, MediaArchive mediaarchive, String searchtype, Page upload, String tempindex) throws Exception{


    		Searcher searcher = mediaarchive.getSearcher(searchtype);
    		if(searcher instanceof ElasticListSearcher){
    			return;
    		}
    		ElasticNodeManager manager = (ElasticNodeManager)mediaarchive.getNodeManager();
    		
    		BulkProcessor processor = manager.getBulkProcessor();
    		
    		try{


    			ZipInputStream unzip = new ZipInputStream(upload.getInputStream());
    			ZipEntry entry = unzip.getNextEntry();



    			MappingJsonFactory f = new MappingJsonFactory();
    			JsonParser jp = f.createParser(new InputStreamReader(unzip, "UTF-8"));

    			JsonToken current;

    			current = jp.nextToken();
    			if (current != JsonToken.START_OBJECT) {
    				System.out.println("Error: root should be object: quiting.");
    				return;
    			}

    			while (jp.nextToken() != JsonToken.END_OBJECT) {
    				String fieldName = jp.getCurrentName();
    				// move from field name to field value
    				current = jp.nextToken();
    				if (fieldName.equals(searchtype)) {
    					if (current == JsonToken.START_ARRAY) {
    						// For each of the records in the array
    						while (jp.nextToken() != JsonToken.END_ARRAY) {
    							// read the record into a tree model,
    							// this moves the parsing position to the end of it
    							JsonNode node = jp.readValueAsTree();
    							IndexRequest req = Requests.indexRequest(tempindex).type(searchtype);
    							String json  = node.toString();
    							
    							req.source(json);
    							JsonNode id = node.get("id");
    							if( id == null)
    							{
    								log.info("No ID found " + searchtype + " node:" + node);
    							}
    							else
    							{
    								req.id(id.asText());
    							}	
    							processor.add(req);

    						}
    					} else {
    						System.out.println("Error: records should be an array: skipping.");
    						jp.skipChildren();
    					}
    				} else {
    					System.out.println("Unprocessed property: " + fieldName);
    					jp.skipChildren();
    				}
    			}
    		}
    		finally{

    			manager.flushBulk();

    				//This is in memory only flush
    				//RefreshResponse actionGet = getClient().admin().indices().prepareRefresh(catid).execute().actionGet();



    		



    	//	}

    	}

    }
    */
    
    
    public void createIndex(RestHighLevelClient client)
    {
    		try
    		{
    			CreateIndexRequest request = new CreateIndexRequest("assets_catalog_asset");
    			request.settings(Settings.builder() 
    				    .put("index.number_of_shards", 3)
    				    .put("index.number_of_replicas", 2)
    				);
    			
    			XContentBuilder builder = XContentFactory.jsonBuilder();
    			builder.startObject();
    			{
    			    builder.startObject("properties");
    			    {
    			        builder.startObject("message");
    			        {
    			            builder.field("type", "text");
    			        }
    			        builder.endObject();
    			    }
    			    builder.endObject();
    			}
    			builder.endObject();
    			request.mapping(builder);
    			
    			//request.alias(new Alias("twitter_alias").filter(QueryBuilders.termQuery("user", "kimchy")));
    			CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
    			
    			//client.search(searchRequest, options);
    			
    		}
    		catch (Throwable ex)
    		{
    			ex.printStackTrace();
    		}
    }
}
