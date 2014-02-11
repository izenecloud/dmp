package com.b5m.scd;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.List;

/**
 * Filters SCD files in a directory.
 * @author Paolo D'Apice
 */
public final class ScdFileFilter implements FileFilter {

    @Override
    public boolean accept(File file) {
        return file.isFile() && file.getName().matches("^.+\\.(SCD|scd)$");
    }

    /**
     * Get a list of SCD files in dir.
     */
    public static List<File> scdFilesIn(File dir) {
        return Arrays.asList(dir.listFiles(new ScdFileFilter()));
    }

}

