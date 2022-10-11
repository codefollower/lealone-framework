/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.write.multiRowsUpdate;

import java.sql.Connection;
import java.sql.Statement;

import org.lealone.bench.cs.write.ClientServerWriteBTest;

public abstract class MultiRowsUpdateBTest extends ClientServerWriteBTest {

    private int rowCount = threadCount;
    private String[] sqls = new String[rowCount];
    private String[] sqlsWarmUp = new String[rowCount];

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists MultiRowsUpdateBTest");
        String sql = "create table if not exists MultiRowsUpdateBTest(pk int primary key, f1 int)";
        statement.executeUpdate(sql);

        for (int row = 1; row <= rowCount; row++) {
            sql = "insert into MultiRowsUpdateBTest values(" + row + ",1)";
            statement.executeUpdate(sql);
        }
        for (int i = 1; i <= rowCount; i++) {
            sqls[i - 1] = "update MultiRowsUpdateBTest set f1=10 where pk=" + i;
        }
        for (int i = 1; i <= rowCount; i++) {
            sqlsWarmUp[i - 1] = "update MultiRowsUpdateBTest set f1=20 where pk=" + i;
        }
        close(statement, conn);
    }

    @Override
    protected UpdateThreadBase createUpdateThread(int id, Connection conn) {
        return new UpdateThread(id, conn);
    }

    private class UpdateThread extends UpdateThreadBase {
        String sql;
        String sqlWarmUp;

        UpdateThread(int id, Connection conn) {
            super(id, conn);
            this.sql = sqls[id];
            this.sqlWarmUp = sqlsWarmUp[id];
        }

        @Override
        public void warmUp() throws Exception {
            for (int i = 0; i < sqlCountPerLoop * 2; i++)
                stmt.executeUpdate(sqlWarmUp);
        }

        @Override
        protected String nextSql() {
            return sql;
        }
    }
}
