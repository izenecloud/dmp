package com.b5m.maxent;

import java.io.File;
import java.io.FileFilter;

/**
 * Filter files with ".out" extension.
 *
 * @author Paolo D'Apice
 */
final class OutFileFilter implements FileFilter {

    @Override
    public boolean accept(File file) {
        return file.isFile() && file.getName().endsWith(".out");
    }

}

