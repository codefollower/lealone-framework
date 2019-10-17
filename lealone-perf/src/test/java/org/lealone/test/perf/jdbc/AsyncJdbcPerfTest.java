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

import java.sql.ResultSet;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

public class AsyncJdbcPerfTest extends JdbcPerfTest {

    public static void main(String[] args) throws Exception {
        new AsyncJdbcPerfTest().run();
    }

    @Override
    protected void write(JdbcStatement stmt, int start, int end) throws Exception {
        for (int i = start; i < end; i++) {
            String sql = "INSERT INTO JdbcPerfTest(f1, f2) VALUES(" + i + "," + i * 10 + ")";
            stmt.executeUpdateAsync(sql, res -> {
                notifyOperationComplete();
            });
        }
    }

    @Override
    protected void read(JdbcStatement stmt, int start, int end, boolean random) throws Exception {
        AsyncHandler<AsyncResult<ResultSet>> handler = ac -> {
            ResultSet rs = ac.getResult();
            try {
                // TODO 有可能为null吗？
                if (rs != null)
                    while (rs.next()) {
                        // System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                    }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                notifyOperationComplete();
            }
        };
        for (int i = start; i < end; i++) {
            if (!random)
                stmt.executeQueryAsync("SELECT * FROM JdbcPerfTest where f1 = " + i, handler);
            else
                stmt.executeQueryAsync("SELECT * FROM JdbcPerfTest where f1 = " + this.random.nextInt(end), handler);
        }
    }
}
