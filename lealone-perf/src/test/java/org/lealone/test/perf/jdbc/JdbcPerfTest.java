/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.perf.jdbc;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Random;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.test.perf.PerfTestBase;

//测试同步和异步jdbc api的性能
public abstract class JdbcPerfTest extends PerfTestBase {

    protected final Random random = new Random();

    protected abstract void write(JdbcStatement stmt, int start, int end) throws Exception;

    protected abstract void read(JdbcStatement stmt, int start, int end, boolean random) throws Exception;

    @Override
    public void run(int loop) throws Exception {
        init();
        super.run(loop);
    }

    @Override
    protected void resetFields() {
        super.resetFields();
        pendingOperations.set(rowCount * 3);
    }

    @Override
    protected void init() throws Exception {
        Connection conn = getLealoneConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS JdbcPerfTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS JdbcPerfTest (f1 int primary key, f2 long)");
        stmt.close();
        conn.close();
    }

    @Override
    protected PerfTestTask createPerfTestTask(int start, int end) throws Exception {
        return new JdbcPerfTestTask(start, end);
    }

    private class JdbcPerfTestTask extends PerfTestTask {
        final Connection conn;
        final JdbcStatement stmt;

        JdbcPerfTestTask(int start, int end) throws Exception {
            super(start, end);
            this.conn = getLealoneConnection();
            this.stmt = (JdbcStatement) conn.createStatement();
        }

        @Override
        public void startPerfTest() throws Exception {
            write(stmt, start, end);
            read(stmt, start, end, false);
            read(stmt, start, end, true);
        }

        @Override
        public void stopPerfTest() throws Exception {
            stmt.close();
            conn.close();
        }
    }
}
