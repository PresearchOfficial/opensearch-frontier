package net.presearch.urlfrontier;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;

public class IndexCreation {

    public static void checkOrCreateIndex(RestHighLevelClient client, String indexName, Logger log)
            throws IOException {
        boolean indexExists =
                client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        log.info("Index '{}' exists? {}", indexName, indexExists);
        if (!indexExists) {
            boolean created = IndexCreation.createIndex(client, indexName, indexName + ".mapping");
            log.info("Index '{}' created? {}", indexName, created);
        }
    }

    public static boolean createIndex(
            RestHighLevelClient client, String indexName, String resourceName) {

        try {

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

            URL mapping = Thread.currentThread().getContextClassLoader().getResource(resourceName);

            String jsonIndexConfiguration = Resources.toString(mapping, Charsets.UTF_8);

            createIndexRequest.source(jsonIndexConfiguration, XContentType.JSON);

            CreateIndexResponse createIndexResponse =
                    client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            return createIndexResponse.isAcknowledged();
        } catch (IOException e) {
            return false;
        }
    }
}
