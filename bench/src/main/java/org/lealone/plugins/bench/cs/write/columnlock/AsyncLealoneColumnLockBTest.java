/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.cs.write.columnlock;

public class AsyncLealoneColumnLockBTest extends ColumnLockBTest {

    public static void main(String[] args) {
        new AsyncLealoneColumnLockBTest().start();
    }
}
