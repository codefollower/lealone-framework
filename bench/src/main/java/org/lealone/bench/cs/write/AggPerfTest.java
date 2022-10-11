/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.write;

import java.sql.Connection;
import java.sql.Statement;

public class AggPerfTest {

    public static void main(String[] args) throws Exception {
        Connection conn;
        conn = org.lealone.bench.cs.ClientServerBTest.getMySQLConnection();
        Statement stmt = conn.createStatement();
        // init(stmt);
        // insert(stmt);
        query(stmt);

        stmt.close();
        conn.close();
    }

    static void insert(Statement stmt) throws Exception {
        for (int i = 0; i < 100; i++) {
            stmt.executeUpdate("INSERT INTO test VALUES('a', 1,10)");
        }
    }

    static void init(Statement stmt) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS test");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test(name varchar(20), f1 int, f2 int)");
    }

    static void query(Statement stmt) throws Exception {
        // stmt.executeUpdate("set QUERY_CACHE_SIZE 0");
        // // stmt.executeUpdate("set olap_threshold 1");
        // stmt.executeUpdate("set olap_batch_size 128");
        // stmt.executeUpdate("set olap_batch_size 256");

        String sql = "SELECT count(*), sum(f1+f2) FROM test";

        for (int i = 0; i < 100; i++) {
            querySync(stmt, sql);
        }
    }

    static void querySync(Statement stmt, String sql) throws Exception {
        int count = 1;
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            stmt.executeQuery(sql).close();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("time: " + ((t2 - t1) / count) + "ms");
    }
}
