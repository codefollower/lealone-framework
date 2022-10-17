/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.polyglot.test;

import org.lealone.main.Lealone;

public class PythonServerStart {

    public static void main(String[] args) {
        args = new String[] { "-baseDir", "target/test-data" };
        Lealone.main(args);
    }
}
