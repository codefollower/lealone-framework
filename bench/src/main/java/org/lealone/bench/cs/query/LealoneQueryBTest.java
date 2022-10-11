/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.query;

import java.sql.Connection;
import java.sql.Statement;

import org.lealone.xsql.postgresql.server.PgServer;

public class LealoneQueryBTest extends QueryBTest {

    public static void main(String[] args) throws Throwable {
        Connection conn = getConnection(PgServer.DEFAULT_PORT, "test", "test");
        Statement statement = conn.createStatement();
        statement.executeUpdate("set QUERY_CACHE_SIZE 0;");

        run("LealoneQuery", statement);
        statement.close();
        conn.close();
    }
}
