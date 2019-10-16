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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public abstract class SqlPerfTest {

    int loopCount = 10;
    int rowCount = 10000;
    int threadCount = 4;
    int[] randomKeys = getRandomKeys();

    int[] getRandomKeys() {
        int count = rowCount / 10;
        ArrayList<Integer> list = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            list.add(i);
        }
        Collections.shuffle(list);
        int[] keys = new int[count];
        for (int i = 0; i < count; i++) {
            keys[i] = list.get(i);
        }
        return keys;
    }

    abstract Connection getConnection() throws Exception;

    public void run(String[] args) throws Exception {
        Connection conn = getConnection();
        init(conn, rowCount);
        run0(args);
        conn.close();
    }

    public void run0(String[] args) throws Exception {
        int avg = rowCount / 10 / threadCount;
        avg = rowCount / threadCount;
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < loopCount; i++) {
            long t11 = System.currentTimeMillis();
            CountDownLatch latch = new CountDownLatch(threadCount);
            for (int t = 0; t < threadCount; t++) {
                int start = t * avg;
                int end = (t + 1) * avg;
                if (t == threadCount - 1)
                    end = rowCount;
                int end2 = end;
                new Thread(() -> {
                    Connection conn;
                    try {
                        conn = getConnection();
                        // init(conn, rowCount);

                        // for (int i = 0; i < loopCount; i++) {
                        update(conn, rowCount / 10, start, end2);
                        // }
                        conn.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }
            latch.await();
            long t22 = System.currentTimeMillis();
            System.out.println("loop: " + (i + 1) + ", run row count: " + rowCount + ", time: " + (t22 - t11) + " ms");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("run row count: " + rowCount + ",total time: " + (t2 - t1) + " ms");
    }

    void update(Connection conn, int rowCount, int start, int end) throws Exception {
        // int[] randomKeys = getRandomKeys();
        // int rowCount2 = end - start;
        Statement statement = conn.createStatement();
        // long t1 = System.currentTimeMillis();
        for (int i = start; i < end; i++) {
            int f1 = i;
            // for (int i = 0; i < rowCount; i++) {
            // int f1 = randomKeys[i];
            String sql = "update SqlPerfTest set f2 = 'pet2' where f1 =" + f1;
            statement.executeUpdate(sql);
        }
        // long t2 = System.currentTimeMillis();
        // System.out.println("update row count: " + rowCount2 + ", time: " + (t2 - t1) + " ms");
        statement.close();
    }

    void init(Connection conn, int rowCount) throws Exception {
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table IF EXISTS SqlPerfTest");
        statement.executeUpdate("create table IF NOT EXISTS SqlPerfTest(f1 int primary key , f2 varchar(20))");
        // statement.executeUpdate("create index index0 on SqlPerfTest(f2)");
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            statement.executeUpdate("insert into SqlPerfTest values(" + i + ",'value-" + i + "')");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("insert rowCount: " + rowCount + ", time: " + (t2 - t1) + " ms");
        statement.close();
    }
}
