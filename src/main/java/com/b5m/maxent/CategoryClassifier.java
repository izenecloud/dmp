package com.b5m.maxent;

/**
 * Interface for the category classifier.
 * @author Paolo D'Apice
 */
public interface CategoryClassifier {

    /**
     * Associates a title string to a category.
     */
    String getCategory(String title);

}

