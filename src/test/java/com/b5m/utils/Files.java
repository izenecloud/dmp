package com.b5m.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Collection of utility methods for dealing with files and directories.
 */
public class Files {

    /** Gets resource file with given name. */
    public static File getResource(String name) throws IOException {
        URL url =  Files.class.getResource(name);
        return new File(url.getFile());
    }

    /** Creates a temporary directory. */
    public static File tempDir(String name) {
        File dir = new File(systemTmpDir(), name);
        dir.mkdir();
        return dir;
    }

    /** Gets the default temporary directory. */
    public static File systemTmpDir() {
        return new File(System.getProperty("java.io.tmpdir"));
    }

}
