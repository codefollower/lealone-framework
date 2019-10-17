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
package org.lealone.test.perf.sql;

import java.sql.Connection;
import java.sql.Statement;

import org.lealone.test.perf.PerfTestBase;

public abstract class SqlPerfTest extends PerfTestBase {

    protected abstract Connection getConnection() throws Exception;

    @Override
    protected void init() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("drop table IF EXISTS SqlPerfTest");
        stmt.executeUpdate("create table IF NOT EXISTS SqlPerfTest(f1 int primary key , f2 varchar(20))");
        // stmt.executeUpdate("create index index0 on SqlPerfTest(f2)");
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            stmt.executeUpdate("insert into SqlPerfTest values(" + i + ",'value-" + i + "')");
        }
        long t2 = System.currentTimeMillis();
        printResult("insert row count: " + rowCount + ", total time: " + (t2 - t1) + " ms");
        stmt.close();
        conn.close();
    }

    protected void update(Statement stmt, int start, int end) throws Exception {
        for (int i = start; i < end; i++) {
            int f1 = i;
            if (isRandom())
                f1 = randomKeys[i];
            String sql = "update SqlPerfTest set f2 = 'value2' where f1 =" + f1;
            stmt.executeUpdate(sql);
            notifyOperationComplete();
        }
    }

    @Override
    protected SqlPerfTestTask createPerfTestTask(int start, int end) throws Exception {
        return new SqlPerfTestTask(start, end);
    }

    protected class SqlPerfTestTask extends PerfTestTask {
        final Connection conn;
        final Statement stmt;

        protected SqlPerfTestTask(int start, int end) throws Exception {
            super(start, end);
            this.conn = getConnection();
            this.stmt = conn.createStatement();
        }

        @Override
        public void startPerfTest() throws Exception {
            update(stmt, start, end);
        }

        @Override
        public void stopPerfTest() throws Exception {
            stmt.close();
            conn.close();
        }
    }
}
