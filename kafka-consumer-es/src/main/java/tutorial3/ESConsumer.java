package tutorial3;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ESConsumer {

    public static RestHighLevelClient createClient() {
        String hostname = "localhost";
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));
        return new RestHighLevelClient(builder);
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ESConsumer.class.getName());
        RestHighLevelClient client = createClient();
        String json = "{\"foo\" : \"bar\"}";
        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(json, XContentType.JSON);
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = response.getId();
        logger.info("id: " + id);
        client.close();
    }
}
