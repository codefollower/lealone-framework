/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.query;

import java.sql.Connection;
import java.sql.Statement;

public class PgQueryBTest extends QueryBTest {

    public static void main(String[] args) throws Throwable {
        Connection conn = getConnection(5432, "postgres", "zhh");
        Statement statement = conn.createStatement();

        run("PgQuery", statement);
        statement.close();
        conn.close();
    }
}
