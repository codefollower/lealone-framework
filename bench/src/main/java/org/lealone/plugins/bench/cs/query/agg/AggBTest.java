/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.cs.query.agg;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.lealone.plugins.bench.cs.query.ClientServerQueryBTest;

public abstract class AggBTest extends ClientServerQueryBTest {

    public AggBTest() {
        threadCount = 8;
        outerLoop = 15;
        innerLoop = 5;
        sqlCountPerInnerLoop = 5;
        rowCount = 10000;
        // printInnerLoopResult = true;
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists AggBTest");
        String sql = "create table if not exists AggBTest(pk int primary key, f1 int)";
        statement.executeUpdate(sql);

        sql = "insert into AggBTest values(?,1)";
        PreparedStatement ps = conn.prepareStatement(sql);

        for (int row = 1; row <= rowCount; row++) {
            ps.setInt(1, row);
            ps.addBatch();
            if (row % 100 == 0 || row == rowCount) {
                ps.executeBatch();
                ps.clearBatch();
            }
        }
        close(statement, ps, conn);
    }

    protected abstract String nextAggSql();

    @Override
    protected QueryThreadBase createQueryThread(int id, Connection conn) {
        return new QueryThread(id, conn);
    }

    private class QueryThread extends QueryThreadBase {

        QueryThread(int id, Connection conn) {
            super(id, conn);
        }

        @Override
        protected String nextSql() {
            return nextAggSql();
        }
    }
}
