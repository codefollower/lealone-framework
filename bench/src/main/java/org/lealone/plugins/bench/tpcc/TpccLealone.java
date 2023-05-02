/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.tpcc;

import org.lealone.plugins.bench.tpcc.codefutures.Tpcc;

public class TpccLealone {
    public static void main(String[] args) {
        System.setProperty("tpcc.config", "lealone/tpcc.properties");
        // TpccLoad.main(args);
        Tpcc.main(args);
    }
}
