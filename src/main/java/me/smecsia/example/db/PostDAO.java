package me.smecsia.example.db;

import me.smecsia.example.model.Post;
import me.smecsia.example.service.MorphiaDBService;
import org.mongodb.morphia.dao.BasicDAO;

/**
 * @author smecsia
 */
public class PostDAO extends BasicDAO<Post, String> {

    public PostDAO(MorphiaDBService dbService) {
        super(dbService.getDatastore());
    }
}
