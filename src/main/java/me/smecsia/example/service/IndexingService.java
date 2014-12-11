package me.smecsia.example.service;

import me.smecsia.example.model.IndexingResult;

import java.util.List;

/**
 * @author Ilya Sadykov
 */
public interface IndexingService {
    List<IndexingResult> search(Class modelClass, String value);

    List<IndexingResult> search(String collectionName, String value);

    void addToIndex(Class modelClass);

    void addToIndex(String collectionName);
}
