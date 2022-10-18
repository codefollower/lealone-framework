/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql;

import java.io.File;
import java.io.IOException;

import org.lealone.storage.fs.FileUtils;

public class DeletePgTestData {

    public static void main(String[] args) throws IOException {
        String dir = "./target/data";
        FileUtils.deleteRecursive(dir, true);
        if (!FileUtils.exists(dir)) {
            System.out.println("dir '" + new File(dir).getCanonicalPath() + "' deleted");
        }
    }

}
