package me.smecsia.example.service;

import me.smecsia.example.model.IndexingResult;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static jodd.io.FileUtil.createTempDirectory;
import static jodd.io.FileUtil.deleteDir;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ElasticSearchService implements EmbeddedService, IndexingService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Node node;
    private final String mongoReplicaSet;
    private final String dataDirectory;
    private final String mongoDBName;
    private final String username;
    private final String password;
    private final int initTimeout;
    private final boolean removeDataDir;
    private final boolean enabled;
    private volatile boolean stopped = false;

    public ElasticSearchService(
            String mongoReplicaSet,
            String mongoDatabaseName,
            String mongoUsername,
            String mongoPassword,
            String dataDirectory,
            boolean enabled,
            int initTimeout
    ) throws IOException {
        this.mongoReplicaSet = mongoReplicaSet;
        this.mongoDBName = mongoDatabaseName;
        this.enabled = enabled;
        this.username = mongoUsername;
        this.password = mongoPassword;
        this.initTimeout = initTimeout;

        if (isEmpty(dataDirectory) || dataDirectory.equals("TMP")) {
            this.removeDataDir = true;
            this.dataDirectory = createTempDirectory("elastic", "data").getPath();
        } else {
            this.dataDirectory = dataDirectory;
            this.removeDataDir = false;
        }
    }

    @Override
    public void start() {
        if (this.enabled) {
            logger.info("Starting the embedded elasticsearch service...");
            ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                    .put("http.enabled", "false")
                    .put("path.data", dataDirectory);

            this.node = nodeBuilder().local(true).settings(elasticsearchSettings.build()).node();
        } else {
            this.node = null;
        }
    }

    @Override
    public void stop() {
        if (!stopped) {
            logger.info("Shutting down the embedded elasticsearch service...");
            stopped = true;
            if (node != null) {
                node.stop();
                node.close();
                node = null;
            }
            if (removeDataDir) {
                try {
                    deleteDir(new File(dataDirectory));
                } catch (Exception e) {
                    logger.error("Failed to remove data dir", e);
                }
            }
        }
    }

    @Override
    public List<IndexingResult> search(Class modelClass, String value) {
        return search(collectionName(modelClass), value);
    }

    @Override
    public List<IndexingResult> search(String collectionName, String value) {
        final List<IndexingResult> results = new ArrayList<>();
        if (enabled) {
            logger.debug(format("Searching for '%s' in collection '%s' ...", value, collectionName));
            final SearchResponse resp = search(collectionName, queryString(value));
            for (SearchHit hit : resp.getHits()) {
                results.add(new IndexingResult(hit.getId(), hit.score(), hit.getSource()));
            }
            logger.debug(format("Search for '%s' in collection '%s' gave %d results...",
                    value, collectionName, results.size()));
        }
        return results;
    }

    @Override
    public void addToIndex(Class modelClass) {
        addToIndex(collectionName(modelClass));
    }

    @Override
    public void addToIndex(String collectionName) {
        try {
            logger.debug(format("Adding collection '%s' to the embedded ElasticSearch index...", collectionName));
            indexCollection(collectionName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to index collection", e);
        }
    }

    public Client getClient() {
        return node.client();
    }

    private SearchResponse search(String collectionName, QueryBuilder query) {
        final CountResponse count = count(collectionName, query);
        return getClient().prepareSearch().setTypes(collectionName)
                .setQuery(query)
                .setSize((int) count.getCount())
                .addFields("id")
                .execute()
                .actionGet();
    }

    private CountResponse count(String collectionName, QueryBuilder query) {
        return getClient().prepareCount().setTypes(collectionName)
                .setQuery(query)
                .execute()
                .actionGet();
    }

    private String collectionName(Class modelClass) {
        return modelClass.getSimpleName().toLowerCase();
    }

    private void indexCollection(String collectionName) throws IOException {
        if (enabled) {
            final XContentBuilder config = jsonBuilder()
                    .startObject()
                        .field("type", "mongodb")
                        .startObject("mongodb");
                            config
                            .startArray("servers");
                                for (String replSetEl : mongoReplicaSet.split(",")) {
                                    final String[] hostPort = replSetEl.split(":");
                                    config
                                            .startObject()
                                                .field("host", hostPort[0])
                                                .field("port", Integer.parseInt(hostPort[1]))
                                            .endObject();
                                }
                                config
                            .endArray();
                                config
                            .startArray("credentials")
                                .startObject()
                                    .field("db", "local")
                                    .field("auth", mongoDBName)
                                    .field("user", username)
                                    .field("password", password)
                                .endObject()
                                .startObject()
                                    .field("db", mongoDBName)
                                    .field("auth", mongoDBName)
                                    .field("user", username)
                                    .field("password", password)
                                .endObject()
                            .endArray();
                                config
                            .field("db", mongoDBName)
                            .field("collection", collectionName)
                            .field("gridfs", false)
                            .startObject("options")
                                    .field("secondary_read_preference", "false")
                                    .field("drop_collection", "true")
                                    .field("is_mongos", "false")
                            .endObject()
                        .endObject()
                        .startObject("index")
                            .field("name", mongoDBName)
                            .field("type", collectionName)
                            .field("bulk_size", "1000")
                            .field("bulk_timeout", "30")
                        .endObject()
                    .endObject();
            getClient().prepareIndex("_river", collectionName, "_meta").setSource(config)
                    .execute().actionGet(initTimeout);
        }
    }
}
