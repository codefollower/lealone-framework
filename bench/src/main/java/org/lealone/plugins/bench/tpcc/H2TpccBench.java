/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.tpcc;

public class H2TpccBench {
    public static void main(String[] args) {
        System.setProperty("tpcc.config", "h2/tpcc.properties");
        TpccBench.main(args);
    }
}
