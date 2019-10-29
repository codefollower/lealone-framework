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
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.lealone.client.jdbc.JdbcPreparedStatement;
import org.lealone.client.jdbc.JdbcStatement;

public class LealoneSqlPerfTest extends SqlPerfTest {

    public static void main(String[] args) throws Exception {
        new LealoneSqlPerfTest().run(args);
    }

    @Override
    protected Connection getConnection() throws Exception {
        return getLealoneConnection();
    }

    @Override
    protected void update(Statement stmt, int start, int end) throws Exception {
        JdbcStatement statement = (JdbcStatement) stmt;
        for (int i = start; i < end; i++) {
            int f1 = i;
            if (isRandom())
                f1 = randomKeys[i];
            statement.executeUpdateAsync("update SqlPerfTest set f2 = 'value2' where f1 =" + f1, ar -> {
                notifyOperationComplete();
            });
        }
    }

    @Override
    protected void prepare(PreparedStatement ps, int start, int end) throws Exception {
        JdbcPreparedStatement ps2 = (JdbcPreparedStatement) ps;
        for (int i = start; i < end; i++) {
            int f1 = i;
            if (isRandom())
                f1 = randomKeys[i];
            ps2.setInt(1, f1);
            ps2.executeUpdateAsync(ar -> {
                notifyOperationComplete();
            });
        }
    }
}
