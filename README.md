# MongoDB-Elasticsearch embedded example

This project shows an example of how to use the [embedded MongoDB](https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo),
[Elasticsearch](https://github.com/elasticsearch/elasticsearch) and [elasticsearch-river-mongodb](https://github.com/richardwilly98/elasticsearch-river-mongodb)
within the single project.

To see how it works, please take a look at the [test](https://github.com/smecsia/mongo-elastic-embedded-example/blob/master/src/test/java/me/smecsia/example/service/ElasticSearchServiceTest.java):

```java
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

```

