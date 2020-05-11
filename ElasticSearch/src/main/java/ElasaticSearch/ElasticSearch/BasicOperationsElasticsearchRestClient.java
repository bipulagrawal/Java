package ElasaticSearch.ElasticSearch;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

public class BasicOperationsElasticsearchRestClient {

	private RestClient client;

	public RestClient getClient() {
		return client;
	}

	public BasicOperationsElasticsearchRestClient() {
		client = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
	}

	public Response indexDocument(String index, String type, String id, String json) throws IOException {
		Map<String, String> params = Collections.emptyMap();
		HttpEntity entity = new NStringEntity(json, ContentType.APPLICATION_JSON);
		Response response = getClient().performRequest("PUT", "/posts/doc/1", params, entity);

		return response;
	}

	public void insertDoc() {
		BasicOperationsElasticsearchRestClient example = new BasicOperationsElasticsearchRestClient();

		String json = "{" + "\"user\":\"bipul\"," + "\"postDate\":\"2020-05-11\","
				+ "\"message\":\"trying out Elasticsearch\"" + "}";
		try {
			Response response = example.indexDocument("posts", "doc", "1", json);
			System.out.println(response.toString());
			example.getClient().close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void returnSpecificDoc() {
		HttpEntity entity1 = new NStringEntity(
				"{\n" + "    \"query\" : {\n" + "    \"match\": { \"user\":\"john\"} \n" + "} \n" + "}",
				ContentType.APPLICATION_JSON);

		try {
			Response response = getClient().performRequest("GET", "/posts/_search",
					Collections.singletonMap("pretty", "true"), entity1);
			System.out.println(EntityUtils.toString(response.getEntity()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void returnAllDoc() {
		try {
			Response response = getClient().performRequest("GET", "/posts/_search",
					Collections.singletonMap("pretty", "true"));
			System.out.println(EntityUtils.toString(response.getEntity()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		BasicOperationsElasticsearchRestClient s = new BasicOperationsElasticsearchRestClient();
		// s.insertDoc();
		// s.returnSpecificDoc();
		s.returnAllDoc();
	}
}