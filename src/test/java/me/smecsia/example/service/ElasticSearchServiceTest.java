package me.smecsia.example.service;

import me.smecsia.example.db.PostDAO;
import me.smecsia.example.model.IndexingResult;
import me.smecsia.example.model.Post;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import static com.mongodb.ReadPreference.nearest;
import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static me.smecsia.example.service.IndexingServiceMatcher.findIndexedAtLeast;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static ru.yandex.qatools.matchers.decorators.MatcherDecoratorsBuilder.should;
import static ru.yandex.qatools.matchers.decorators.TimeoutWaiter.timeoutHasExpired;

/**
 * @author smecsia
 */
public class ElasticSearchServiceTest {
    public static final String RS_NAME = "local";
    public static final String RS = "localhost:37017";
    public static final String DB = "mongolastic";
    public static final String USER = "user";
    public static final String PASS = "pass";
    ElasticSearchService es;
    MongoDBService mongo;
    PostDAO postDAO;

    @Before
    public void startEmbeddedServers() throws IOException, InterruptedException {
        mongo = new MongoDBService(RS, DB, USER, PASS, RS_NAME, null, true);
        mongo.setRoles("\"readWrite\"","{\"db\":\"local\",\"role\":\"read\"}");
        mongo.start();
        es = new ElasticSearchService(RS, DB, USER, PASS, null, true, 25000);
        es.start();

        final MorphiaDBService dbService = new MorphiaDBService(RS, DB, USER, PASS);
        dbService.getDatastore().getDB().getMongo().setReadPreference(nearest());
        dbService.getDatastore().setDefaultWriteConcern(ACKNOWLEDGED);
        postDAO = new PostDAO(dbService);
    }

    @After
    public void shutdownEmbeddedServers() throws IOException {
        mongo.stop();
        es.stop();
    }

    @Test
    public void testElasticFullTextSearch() throws IOException, InterruptedException {
        // create some test data
        Post post1 = createPost("Some title", "Some post with keyword among other words");
        Post post2 = createPost("Some another title", "Some post without the required word");
        Post post3 = createPost("Some third title", "Some post with the required keyword among other words");

        es.addToIndex(Post.class); // create the new index within elastic using the mongodb river

        assertThat("At least two posts must be found by query",
                es, should(findIndexedAtLeast(Post.class, "body:keyword", 2))
                        .whileWaitingUntil(timeoutHasExpired(20000)));

        // perform the search
        final List<IndexingResult> response = es.search(Post.class, "body:keyword");
        assertThat(response, hasSize(2));
        assertThat(response.get(0).getId(), is(post1.getId().toString()));
        assertThat(response.get(1).getId(), is(post3.getId().toString()));
    }


    private Post createPost(String title, String description) throws UnknownHostException {
        final Post post = new Post();
        post.setTitle(title);
        post.setBody(description);
        postDAO.save(post);
        return post;
    }

}
