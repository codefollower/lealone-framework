/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.tpcc;

import org.lealone.plugins.bench.tpcc.codefutures.Tpcc;

public class TpccMySQL {
    public static void main(String[] args) {
        System.setProperty("tpcc.config", "mysql/tpcc.properties");
        // TpccLoad.main(args);
        Tpcc.main(args);
    }
}
