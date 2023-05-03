/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.tpcc;

import org.lealone.plugins.bench.tpcc.codefutures.TpccBench;

public class LealoneTpccBench {
    public static void main(String[] args) {
        System.setProperty("tpcc.config", "lealone/tpcc.properties");
        TpccBench.main(args);
    }
}
