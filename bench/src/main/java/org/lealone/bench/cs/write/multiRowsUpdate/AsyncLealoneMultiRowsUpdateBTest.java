/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.write.multiRowsUpdate;

public class AsyncLealoneMultiRowsUpdateBTest extends MultiRowsUpdateBTest {

    public static void main(String[] args) {
        new AsyncLealoneMultiRowsUpdateBTest().start();
    }
}
