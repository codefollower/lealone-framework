/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.cs.write.rowlock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Random;

import org.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public abstract class RowLockBTest extends ClientServerWriteBTest {

    protected RowLockBTest() {
        rowCount = threadCount;
        sqlCountPerInnerLoop = 50;
        sqls = new String[rowCount];
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists RowLockBTest");
        String sql = "create table if not exists RowLockBTest(pk int primary key, f1 int)";
        statement.executeUpdate(sql);

        sql = "insert into RowLockBTest values(?,1)";
        PreparedStatement ps = conn.prepareStatement(sql);

        for (int row = 1; row <= rowCount; row++) {
            ps.setInt(1, row);
            ps.addBatch();
            if (row % 100 == 0 || row == rowCount) {
                ps.executeBatch();
                ps.clearBatch();
            }
        }
        for (int i = 1; i <= rowCount; i++) {
            sqls[i - 1] = "update RowLockBTest set f1=10 where pk=" + i;
        }
        close(statement, ps, conn);
    }

    @Override
    protected UpdateThreadBase createUpdateThread(int id, Connection conn) {
        return new UpdateThread(id, conn);
    }

    private class UpdateThread extends UpdateThreadBase {

        private final Random random = new Random();

        UpdateThread(int id, Connection conn) {
            super(id, conn);
        }

        @Override
        protected String nextSql() {
            return sqls[random.nextInt(rowCount)];
        }
    }
}
