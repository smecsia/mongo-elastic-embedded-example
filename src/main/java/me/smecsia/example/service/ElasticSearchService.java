package me.smecsia.example.service;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Example implementation of an embedded elasticsearch server.
 *
 * @author Felix Müller
 */
public class ElasticSearchService {

    private final Node node;
    public static final int INIT_TIMEOUT_MS = 5000;
    private final String mongoHost;
    private final int mongoPort;

    public ElasticSearchService( String mongoHost, int mongoPort, String dataDirectory ) {

        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false")
                .put("path.data", dataDirectory);

        this.node = nodeBuilder()
                .local(true)
                .settings(elasticsearchSettings.build())
                .node();
    }

    public Client getClient() {
        return node.client();
    }

    public void shutdown() {
        node.close();
    }

    /**
     * Searches for the indexed values for the model class and query
     */
    public SearchResponse search(Class modelClass, QueryBuilder query){
        return  getClient().prepareSearch().setTypes(collectionName(modelClass))
                .setQuery(query)
                .execute()
                .actionGet();
    }

    /**
     * Registers the new index for the mongo-elastic river and model class
     */
    public void indexCollection(Class modelClass) throws IOException {
        indexCollection(collectionName(modelClass));
    }

    /**
     * Registers the new index for the mongo-elastic river
     */
    protected void indexCollection(String collectionName) throws IOException {
        getClient().prepareIndex("_river", collectionName, "_meta")
                .setSource(jsonBuilder()
                        .startObject()
                            .field("type", "mongodb")
                            .startObject("mongodb")
                                .field("host", mongoHost)
                                .field("port", mongoPort)
                                .field("db", "test")
                                .field("collection", collectionName)
                                .startObject("options")
                                    .field("secondary_read_preference", "true")
                                .endObject()
                            .endObject()
                            .startObject("index")
                                .field("name", collectionName.toLowerCase())
                                .field("type", collectionName)
                                .field("bulk_size", "1000")
                                .field("bulk_timeout", "30")
                            .endObject()
                        .endObject()
                ).execute().actionGet(INIT_TIMEOUT_MS);

    }

    /**
     * Returns the collection name by the model class
     */
    private String collectionName(Class modelClass) {
        return modelClass.getSimpleName();
    }

}
