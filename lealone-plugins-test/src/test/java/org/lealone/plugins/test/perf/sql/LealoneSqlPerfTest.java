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
package org.lealone.plugins.test.perf.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.client.jdbc.JdbcStatement;

public class LealoneSqlPerfTest extends SqlPerfTest {

    public static void main(String[] args) throws Exception {
        new LealoneSqlPerfTest().run(args);
    }

    @Override
    Connection getConnection() throws Exception {
        String url = "jdbc:lealone:tcp://localhost:7210/test;NET_FACTORY_NAME=nio";
        return DriverManager.getConnection(url, "root", "");
    }

    @Override
    void update(Connection conn, int rowCount, int start, int end) throws Exception {
        int rowCount2 = end - start;
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong count = new AtomicLong(rowCount2);
        JdbcStatement statement = (JdbcStatement) conn.createStatement();
        long t1 = System.currentTimeMillis();
        for (int i = start; i < end; i++) {
            int f1 = i;
            // for (int i = 0; i < rowCount; i++) {
            // int f1 = randomKeys[i];
            statement.executeUpdateAsync("update SqlPerfTest set f2 = 'pet2' where f1 =" + f1, ar -> {
                if (count.decrementAndGet() <= 0) {

                    long t2 = System.currentTimeMillis();
                    System.out.println("update row count: " + rowCount2 + ", time: " + (t2 - t1) + " ms");
                    latch.countDown();
                }
            });
        }
        latch.await();
        statement.close();
    }
}
