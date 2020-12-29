package org.entermediadb.loader;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class MainClass
{

	public MainClass()
	{
		// TODO Auto-generated constructor stub
	}
	public static void main(String[] args)
	{
		RestHighLevelClient client = new RestHighLevelClient(
		        RestClient.builder(
		                new HttpHost("154.127.54.122", 9270, "http")
		                //,    new HttpHost("localhost", 9201, "http"))
		             )
		       );
		
		
		
	}
	
}
