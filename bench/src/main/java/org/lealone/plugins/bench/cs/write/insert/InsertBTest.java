/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.cs.write.insert;

import java.sql.Connection;
import java.sql.Statement;

import org.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public abstract class InsertBTest extends ClientServerWriteBTest {

    @Override
    protected void init() throws Exception {
        rowCount = innerLoop * sqlCountPerInnerLoop * threadCount;

        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists InsertBTest");
        String sql = "create table if not exists InsertBTest(pk int primary key, f1 int)";
        statement.executeUpdate(sql);

        for (int i = 1; i <= rowCount; i++) {
            sqls[i - 1] = "insert into InsertBTest values(" + i + ",1)";
        }
        close(statement, conn);
    }

    @Override
    protected UpdateThreadBase createUpdateThread(int id, Connection conn) {
        return new UpdateThread(id, conn);
    }

    private class UpdateThread extends UpdateThreadBase {
        int start;

        UpdateThread(int id, Connection conn) {
            super(id, conn);
            start = innerLoop * sqlCountPerInnerLoop * id;
        }

        @Override
        protected String nextSql() {
            return sqls[start++];
        }
    }
}
