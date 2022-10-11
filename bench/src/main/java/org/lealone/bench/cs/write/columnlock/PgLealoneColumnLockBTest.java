/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.write.columnlock;

import java.sql.Connection;

import org.lealone.xsql.postgresql.server.PgServer;

public class PgLealoneColumnLockBTest extends ColumnLockBTest {

    public static void main(String[] args) {
        new PgLealoneColumnLockBTest().start();
    }

    @Override
    public Connection getConnection() throws Exception {
        return getPgConnection(PgServer.DEFAULT_PORT);
    }
}
