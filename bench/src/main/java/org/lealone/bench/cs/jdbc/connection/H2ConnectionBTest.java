/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.jdbc.connection;

import java.sql.Connection;

public class H2ConnectionBTest extends ConnectionBTest {

    public static void main(String[] args) throws Exception {
        new H2ConnectionBTest().run();
    }

    @Override
    protected Connection getConnection() throws Exception {
        return getH2Connection();
    }
}
