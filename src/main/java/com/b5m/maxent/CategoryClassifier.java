package com.b5m.maxent;

/**
 * Interface for the category classifier.
 * @author Paolo D'Apice
 */
public interface CategoryClassifier {

    /**
     * Associates a title string to a category.
     * @param title Input title
     * @return Category label
     */
    String getCategory(String title);

}

