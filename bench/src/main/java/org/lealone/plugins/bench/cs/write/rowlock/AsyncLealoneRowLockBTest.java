/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.cs.write.rowlock;

public class AsyncLealoneRowLockBTest extends RowLockBTest {

    public static void main(String[] args) {
        new AsyncLealoneRowLockBTest().start();
    }
}
