package me.smecsia.example.service;

import com.mongodb.MongoClient;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.net.UnknownHostException;

/**
 * @author smecsia
 */
public class MorphiaDBService {

    private final Datastore datastore;
    private final MongoClient mongoClient;

    public MorphiaDBService(String host,
                            int port,
                            String dbName) throws UnknownHostException {
        mongoClient = new MongoClient(host, port);
        datastore = new Morphia().createDatastore(mongoClient, dbName);
    }

    public Datastore getDatastore() {
        return datastore;
    }
}
