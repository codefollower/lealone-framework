/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.write.singleRowUpdate;

import java.sql.Connection;
import java.sql.Statement;

import org.lealone.db.Constants;

public class SyncLealoneUpdateBTest extends UpdateBTest {

    public static void main(String[] args) throws Throwable {
        String url = "jdbc:lealone:tcp://localhost:" + Constants.DEFAULT_TCP_PORT + "/lealone";
        Connection conn = getConnection(url, "root", "");
        Statement statement = conn.createStatement();
        statement.executeUpdate("set QUERY_CACHE_SIZE 0;");

        run("SyncLealoneUpdate", statement);
        statement.close();
        conn.close();
    }
}
