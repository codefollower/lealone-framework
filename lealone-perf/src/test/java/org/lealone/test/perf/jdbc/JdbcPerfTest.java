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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.test.perf.PerfTestBase;

//测试同步和异步jdbc api的性能
public abstract class JdbcPerfTest extends PerfTestBase {

    private final int rowCount = 500;
    private final int threadCount = Runtime.getRuntime().availableProcessors();

    protected final AtomicLong startTime = new AtomicLong(0);
    protected final AtomicLong endTime = new AtomicLong(0);
    protected final AtomicLong pendingOperations = new AtomicLong(0);
    protected final Random random = new Random();

    CountDownLatch latch;

    protected abstract void write(JdbcStatement stmt, int start, int end) throws Exception;

    protected abstract void read(JdbcStatement stmt, int start, int end, boolean random) throws Exception;

    private void resetFields() {
        startTime.set(0);
        endTime.set(0);
        pendingOperations.set(rowCount * 3);
        latch = new CountDownLatch(1);
    }

    void notifyOperationComplete() {
        if (pendingOperations.decrementAndGet() <= 0) {
            endTime.set(System.currentTimeMillis());
            latch.countDown();
        }
    }

    @Override
    public void run(int loop) throws Exception {
        resetFields();

        Connection conn = getLealoneConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS JdbcPerfTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS JdbcPerfTest (f1 int primary key, f2 long)");
        stmt.close();

        int avg = rowCount / threadCount;
        MyThread[] threads = new MyThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int start = i * avg;
            int end = (i + 1) * avg;
            if (i == threadCount - 1)
                end = rowCount;
            threads[i] = new MyThread(getLealoneConnection(), start, end);
        }

        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        latch.await();
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].close();
        }
        conn.close();

        long totalTime = endTime.get() - startTime.get();
        long avgTime = totalTime / threadCount;
        printResult(loop, ", rows: " + rowCount + ", threads: " + threadCount + ", total time: " + totalTime
                + " ms, avg time: " + avgTime + " ms");
    }

    private class MyThread extends Thread {
        final Connection conn;
        final JdbcStatement stmt;
        final int start;
        final int end;

        MyThread(Connection conn, int start, int end) throws Exception {
            super("MyThread-" + start);
            this.conn = conn;
            this.stmt = (JdbcStatement) conn.createStatement();
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            // 取最早启动的那个线程的时间
            startTime.compareAndSet(0, System.currentTimeMillis());
            try {
                write(stmt, start, end);
                read(stmt, start, end, false);
                read(stmt, start, end, true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        void close() {
            try {
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
