package me.smecsia.example.service;

import me.smecsia.example.db.PostDAO;
import me.smecsia.example.model.Post;
import org.elasticsearch.action.search.SearchResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import static com.mongodb.ReadPreference.nearest;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.lang.Thread.sleep;
import static jodd.io.FileUtil.createTempDirectory;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author smecsia
 */
public class ElasticSearchServiceTest {
    public static final int MONGO_PORT = 37017;
    public static final int REPLICA_INIT_TIMEOUT_MS = 25000;
    ElasticSearchService es;
    MongoDBService mongo;
    File esDir;
    File dbDir;

    @Before
    public void startEmbeddedServers() throws IOException, InterruptedException {
        esDir = createTempDirectory("elastic", "index");
        dbDir = createTempDirectory("mongodb", "data");
        mongo = new MongoDBService(MONGO_PORT, dbDir.getPath(), "localReplSet", true).start();
        mongo.initiateReplicaSet(REPLICA_INIT_TIMEOUT_MS);
        es = new ElasticSearchService(mongo.net().getBindIp(), mongo.net().getPort(), esDir.getPath());
    }

    @After
    public void shutdownEmbeddedServers() throws IOException {
        es.shutdown();
        deleteDirectory(esDir);
        deleteDirectory(dbDir);
    }

    @Test
    public void testElasticFullTextSearch() throws IOException, InterruptedException {
        // create some test data
        createPost("Some title", "Some post with keyword among other words");
        createPost("Some another title", "Some post without the required word");
        createPost("Some third title", "Some post with the required keyword among other words");

        es.indexCollection(Post.class); // create the new index within elastic using the mongodb river
        sleep(10000); // wait until elastic polls the collection

        // perform the search
        final SearchResponse response = es.search(Post.class, matchPhraseQuery("body", "keyword"));
        assertThat(response.getHits().getTotalHits(), is(2L));
        assertThat((String) response.getHits().getAt(0).getSource().get("title"), is("Some title"));
        assertThat((String) response.getHits().getAt(1).getSource().get("title"), is("Some third title"));
    }

    private void createPost(String title, String description) throws UnknownHostException {
        final String bindIp = mongo.net().getBindIp();
        final int port = mongo.net().getPort();
        final MorphiaDBService dbService = new MorphiaDBService(bindIp, port, "test");
        dbService.getDatastore().getDB().getMongo().setReadPreference(nearest());
        dbService.getDatastore().setDefaultWriteConcern(ACKNOWLEDGED);
        final PostDAO dao = new PostDAO(dbService);
        final Post post = new Post();
        post.setTitle(title);
        post.setBody(description);
        dao.save(post);
    }

}
