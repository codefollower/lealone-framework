/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.bench.cs.write.columnlock;

import java.sql.Connection;
import java.sql.Statement;

import org.lealone.plugins.bench.cs.write.ClientServerWriteBTest;

public abstract class ColumnLockBTest extends ClientServerWriteBTest {

    @Override
    protected void init() throws Exception {
        int columnCount = threadCount;
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists ColumnLockPerfTest");

        StringBuilder buff = new StringBuilder();
        buff.append("create table if not exists ColumnLockPerfTest(pk int primary key");
        for (int i = 1; i <= columnCount; i++) {
            buff.append(",f").append(i).append(" int");
        }
        buff.append(")");
        statement.executeUpdate(buff.toString());

        for (int row = 1; row <= 9; row++) {
            buff = new StringBuilder();
            buff.append("insert into ColumnLockPerfTest values(").append(row);
            for (int i = 1; i <= columnCount; i++) {
                buff.append(",").append(i * 10);
            }
            buff.append(")");
            statement.executeUpdate(buff.toString());
        }
        for (int i = 1; i <= columnCount; i++) {
            buff = new StringBuilder();
            buff.append("update ColumnLockPerfTest set f").append(i).append(" = ").append(i * 1000)
                    .append(" where pk=5");
            sqls[i - 1] = buff.toString();
        }
        close(statement, conn);
    }

    @Override
    protected UpdateThreadBase createUpdateThread(int id, Connection conn) {
        return new UpdateThread(id, conn);
    }

    private class UpdateThread extends UpdateThreadBase {
        String sql;

        UpdateThread(int id, Connection conn) {
            super(id, conn);
            this.sql = sqls[id];
        }

        @Override
        protected String nextSql() {
            return sql;
        }
    }
}
