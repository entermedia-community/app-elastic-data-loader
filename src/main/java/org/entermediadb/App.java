package org.entermediadb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final String GITHOME = "/home/shanti/git/app-elastic-data-loader";
    public static void main( String[] args )
    {
        System.out.println( "Connecting" );
        
        try
        {
        	App app = new App();
        	RestHighLevelClient client = app.connect();
        	//app.setMapping
        	//app.createIndex(client,"assets_catalog4", "asset");
	    	app.loadData(client,"assets_catalog4", "asset");
	    		
	    		// getMapping(client, "twitter");
	    		//insertDocument(client, "posts", "2");
	    		//getDocument(client);
	    		client.close();
		}
		catch (Throwable ex)
		{
			ex.printStackTrace();
		}
    }
    
    private RestHighLevelClient connect()
	{
    	RestHighLevelClient client = new RestHighLevelClient(
		        RestClient.builder(
		                new HttpHost("154.127.54.122", 9270, "http")
		                //,    new HttpHost("localhost", 9201, "http"))
		             )
		 );
		return client;
	}


    public void createIndex(RestHighLevelClient client, String catalogid, String module) throws Exception
    {
			CreateIndexRequest request = new CreateIndexRequest(catalogid + "_" + module);
//			File file = new File(GITHOME + "/data/elasticindex.yaml");
//			FileInputStream stream = new FileInputStream(file);
//			StreamInput input = new InputStreamStreamInput(stream);
//			request.settings(Settings.readSettingsFromStream(input) 				);
	    	String settings = readfile("/home/shanti/git/app-elastic-data-loader/data/elasticindex.yaml");
			request.settings(settings,XContentType.YAML);
			//stream.close();
				//Builder settingsBuilder = Settings.builder().loadFromStream(yaml.getName(), in);
//
//				for (Iterator iterator = getLocalNode().getProperties().keySet().iterator(); iterator.hasNext();)
//				{
//					String key = (String) iterator.next();
//					if (key.startsWith("index.")) //Legacy
//					{
//						String val = getLocalNode().getSetting(key);
//						settingsBuilder.put(key, val);
//					}
//				}
//
//				CreateIndexResponse newindexres = admin.indices().prepareCreate(index).setSettings(settingsBuilder).execute().actionGet();

    			
//    			XContentBuilder builder = XContentFactory.jsonBuilder();
//    			builder.startObject();
//    			{
//    			    builder.startObject("properties");
//    			    {
//    			        builder.startObject("message");
//    			        {
//    			            builder.field("type", "text");
//    			        }
//    			        builder.endObject();
//    			    }
//    			    builder.endObject();
//    			}
//    			builder.endObject();
//    			request.mapping(builder);

    	    	String mapping = readfile("/home/shanti/git/app-elastic-data-loader/data/"+ module + "-mapping.json");
    	    	request.mapping(mapping, XContentType.JSON); 

    			//request.alias(new Alias("twitter_alias").filter(QueryBuilders.termQuery("user", "kimchy")));
    			CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
    	        System.out.println( "complete " + createIndexResponse.toString() );
    			
    			//client.search(searchRequest, options);
    }
    
	public static void insertDocument(RestHighLevelClient client, String index, String id) {
    	Map<String, Object> food = new HashMap<>();
    	food.put("food", "kimchy");
    	food.put("postDate", new Date());
    	food.put("description", "Korean food sec");
    	
    	Map<String, Object> jsonMap = new HashMap<>();
    	jsonMap.put("user", "kimchy");
    	jsonMap.put("postDate", new Date());
    	jsonMap.put("message", "trying out Elasticsearch2");
    	jsonMap.put("food", food);
    	
    	
    	
    	IndexRequest request = new IndexRequest(index).id(id).source(jsonMap);
    	try {
    		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
    	} catch (Throwable ex) {
    		ex.printStackTrace();
    	}
    }
    
    public static void getDocument(RestHighLevelClient client) {
    	GetRequest getRequest = new GetRequest("posts", "1");;
    	
    	try {
    		GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
    		System.out.println(getResponse.toString());
    	} catch (Throwable ex) {
    		ex.printStackTrace();
    	}
    }
    
    
/*    public static void getMapping(RestHighLevelClient client, String index) 
    {
    	try 
    	{
	    	GetMappingsRequest request = new GetMappingsRequest(); 
	    	request.indices("twitter"); 
	    	GetMappingsResponse getMappingResponse = client.indices().getMapping(request, RequestOptions.DEFAULT);
	    	Map<String, MappingMetadata> allMappings = getMappingResponse.mappings(); 
	    	MappingMetadata indexMapping = allMappings.get(index); 
	    	Map<String, Object> mapping = indexMapping.sourceAsMap(); 
	    	System.out.println(mapping.toString());
    	} 
    	catch (Throwable ex) 
    	{
    		ex.printStackTrace();
    	}
    }
  */
    public String readfile(String path) throws Exception
    {
    	StringBuffer output = new StringBuffer();
    	
    	 try (BufferedReader br = new BufferedReader(new InputStreamReader(
                 new FileInputStream(path), StandardCharsets.UTF_8));) {

             String line;
             
             while ((line = br.readLine()) != null) {
                 
            	 output.append(line);
            	 output.append("\n");
             }
         }

         return output.toString();
    }

    /*
    public void setMapping(RestHighLevelClient client, String catalogid, String module) throws Exception 
    {
        System.out.println( "maping mapping "+ catalogid + " " + module );

    	String mapping = readMappingfile(GITHOME + "/data/"+ module + "-mapping.json");
    	
    	PutMappingRequest request = new PutMappingRequest(catalogid + "_" + module); 
    	
//    	XContentBuilder builder = XContentFactory.jsonBuilder();
//    	builder.startObject();
//    	{
//    	    builder.startObject("properties");
//    	    {
//    	        builder.startObject("message");
//    	        {
//    	            builder.field("type", "text");
//    	        }
//    	        builder.endObject();
//    	    }
//    	    builder.endObject();
//    	}
//    	builder.endObject();
    	
    	request.source(mapping, XContentType.JSON); 
    	
    	org.elasticsearch.action.support.master.AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
    }
    */
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
    
    
    public void loadData(RestHighLevelClient client, String catalogid, String module) throws Exception
    {
    		File file = new File( GITHOME + "/data/asset_test.zip");
    		if( !file.exists() )
    		{
    			throw new Exception("Does not exist");
    		}
    		FileInputStream inputfile = new FileInputStream(file );

		    BulkRequest br = new BulkRequest();


			long count = 0;
			try
    		{
    			ZipInputStream unzip = new ZipInputStream(inputfile);
    			ZipEntry entry = unzip.getNextEntry();


    			MappingJsonFactory f = new MappingJsonFactory();
    			JsonParser jp = f.createParser(new InputStreamReader(unzip, "UTF-8"));

    			JsonToken current;

    			current = jp.nextToken();
    			info("Start" + current);
    			if (!JsonToken.START_OBJECT.equals(current))
    			{
    				System.out.println("Error: root should be object: quiting." + current);
    				return;
    			}

    			while (!JsonToken.END_OBJECT.equals(jp.nextToken() ) ) // } != here
    			{
    				String fieldName = jp.getCurrentName();
    				//info("fieldName:" + fieldName);
    				
    				// move from field name to field value
    				current = jp.nextToken();
					if (JsonToken.START_ARRAY.equals(current)) {
						// For each of the records in the array
						while (!JsonToken.END_ARRAY.equals(jp.nextToken()) ) {
							// read the record into a tree model,
							// this moves the parsing position to the end of it
							JsonNode node = jp.readValueAsTree();
							IndexRequest req = Requests.indexRequest(catalogid + "_" + module);
							String json  = node.toString();
							//info(json);
		    		        //client.bulk(br, RequestOptions.DEFAULT);
							req.source(json, XContentType.JSON);
							
							JsonNode id = node.get("id");
							if( id == null)
							{
								info("No ID found " + module + " node:" + node);
							}
							else
							{
								req.id(id.asText());
							}	
							br.add(req);
							count++;
							if( count % 1000 > 0)
							{
								info(" Saved "  + count);
							}
						}
					} else {
						System.out.println("Error: records should be an array: skipping.");
						jp.skipChildren();
					}
    			}
//				} else {
//					System.out.println("Unprocessed property: " + fieldName);
//					jp.skipChildren();
//				}
    		}
    		finally
    		{
    			inputfile.close();
    			BulkResponse bulkResponse = client.bulk(br, RequestOptions.DEFAULT);
    			info( bulkResponse.status().toString() );
    		}
			info(" Complete "  + count);


    }

	private void info(String inString)
	{
		// TODO Auto-generated method stub
		System.out.println(inString);
	}
}
