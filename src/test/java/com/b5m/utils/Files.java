package com.b5m.utils;

import java.io.File;

/**
 * Collection of utility methods for dealing with files and directories.
 */
public class Files {

    /** Creates a temporary directory. */
    public static File tempDir(String name, boolean deleteOnExit) {
        File dir = new File(tmpdir(), name);
        dir.mkdir();
        if (deleteOnExit) dir.deleteOnExit();
        return dir;
    }

    /* system temporary directory */
    private static File tmpdir() {
        return new File(System.getProperty("java.io.tmpdir"));
    }
}
