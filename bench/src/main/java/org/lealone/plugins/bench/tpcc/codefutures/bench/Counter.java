/*
 * Copyright Lealone Database Group. CodeFutures Corporation
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh, CodeFutures Corporation
 */
package org.lealone.plugins.bench.tpcc.codefutures.bench;

public class Counter {

    private long count = 0;

    public Counter() {
    }

    public synchronized long increment() {
        return ++count;
    }

    public synchronized long get() {
        return count;
    }

    public synchronized long reset() {
        long ret = count;
        count = 0;
        return ret;
    }

}
