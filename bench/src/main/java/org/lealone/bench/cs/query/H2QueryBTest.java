/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.query;

import java.sql.Connection;
import java.sql.Statement;

public class H2QueryBTest extends QueryBTest {

    public static void main(String[] args) throws Throwable {
        Connection conn = getConnection(9511, "test", "test");
        Statement statement = conn.createStatement();

        run("H2Query", statement);
        statement.close();
        conn.close();
    }
}
