/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.write.append;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class CasssandraAppendBTest {

    private int loop = 100;
    private int sqlCountPerLoop = 2000;
    private int threadCount = 4;
    private int rowCount = loop * sqlCountPerLoop * threadCount;
    private String[] sqls = new String[rowCount];

    public static void main(String[] args) {
        new CasssandraAppendBTest().run();
    }

    private void run() {
        try (CqlSession session = CqlSession.builder().build()) {
            ResultSet rs = session.execute("select release_version from system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));

            for (int i = 1; i <= rowCount; i++) {
                sqls[i - 1] = "insert into test(f1,f2) values(" + i + ",1)";
            }
            // createKeyspace(session);
            createTable(session);
            try {
                run(threadCount);
            } catch (Exception e) {
                e.printStackTrace();
            }

            rs = session.execute("select f2 from btest.test where f1<10 ALLOW FILTERING");
            List<Row> rows = rs.all();
            System.out.println("row count: " + rows.size());

            rs = session.execute("select count(*) as cnt from btest.test");
            row = rs.one();
            System.out.println("row count: " + row.getLong("cnt"));
        }
    }

    static void createKeyspace(CqlSession session) {
        String sql = " CREATE KEYSPACE btest "
                + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}";
        session.execute(sql);
    }

    private static void createTable(CqlSession session) {
        session.execute("use btest");
        session.execute("DROP TABLE IF EXISTS test");
        session.execute("CREATE TABLE test (f1 int PRIMARY KEY, f2 int)");
    }

    private void run(int threadCount) throws Exception {
        UpdateThread[] threads = new UpdateThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            CqlSession session = CqlSession.builder().build();
            session.execute("use btest");
            threads[i] = new UpdateThread(i, session);
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].warmUp();
        }
        long t1 = System.nanoTime();
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        long t2 = System.nanoTime();
        System.out.println("total time: " + //
                TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
        for (int i = 0; i < threadCount; i++) {
            threads[i].closeSession();
        }
    }

    class UpdateThread extends Thread {
        int start;
        CqlSession session;
        PreparedStatement statement;

        public UpdateThread(int id, CqlSession session) {
            super("Thread-" + id);
            this.session = session;
            statement = session.prepare("insert into test(f1,f2) values(?,1)");
            start = loop * sqlCountPerLoop * id;
        }

        String nextSql() {
            return sqls[start++];
        }

        public void warmUp() throws Exception {
        }

        public void closeSession() throws Exception {
            session.close();
        }

        @Override
        public void run() {
            // executeUpdateSync();
            executeUpdateAsync();
            // executeBatchUpdateAsync();
        }

        protected void executeUpdateSync() {
            for (int j = 0; j < loop; j++) {
                long t1 = System.nanoTime();
                for (int i = 0; i < sqlCountPerLoop; i++) {
                    String sql = nextSql();
                    session.execute(sql);
                }
                long t2 = System.nanoTime();
                System.out.println("CasssandraAppendBTest: " //
                        + TimeUnit.NANOSECONDS.toMicros(t2 - t1) / sqlCountPerLoop);
            }
            System.out.println();
            System.out.println("time: 微秒");
            System.out.println("loop: " + loop + " * " + sqlCountPerLoop);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        protected void executeUpdateAsync() {
            for (int j = 0; j < loop; j++) {
                long t1 = System.nanoTime();
                CountDownLatch latch = new CountDownLatch(sqlCountPerLoop);
                for (int i = 0; i < sqlCountPerLoop; i++) {
                    String sql = nextSql();
                    session.executeAsync(sql).handle(new BiFunction() {
                        @Override
                        public Object apply(Object t, Object u) {
                            latch.countDown();
                            return null;
                        }
                    });
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long t2 = System.nanoTime();
                System.out.println("CasssandraAppendBTest: " //
                        + TimeUnit.NANOSECONDS.toMicros(t2 - t1) / sqlCountPerLoop);
            }
            System.out.println();
            System.out.println("time: 微秒");
            System.out.println("loop: " + loop + " * " + sqlCountPerLoop);
        }

        protected void executeBatchUpdateAsync() {
            for (int j = 0; j < loop; j++) {

                long t1 = System.nanoTime();
                BatchStatement batchStatement = BatchStatement.newInstance(BatchType.UNLOGGED);
                for (int i = 0; i < 5; i++) {
                    BoundStatement boundStmt = statement.bind();
                    boundStmt.setInt(0, start++);
                    batchStatement.add(boundStmt);
                }
                session.execute(batchStatement);

                long t2 = System.nanoTime();
                System.out.println("CasssandraAppendBTest: " //
                        + TimeUnit.NANOSECONDS.toMicros(t2 - t1) / sqlCountPerLoop);
            }
            System.out.println();
            System.out.println("time: 微秒");
            System.out.println("loop: " + loop + " * " + sqlCountPerLoop);
        }
    }
}
