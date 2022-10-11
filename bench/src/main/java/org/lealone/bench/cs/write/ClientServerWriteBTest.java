/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.write;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.lealone.bench.DbType;
import org.lealone.bench.cs.ClientServerBTest;
import org.lealone.client.jdbc.JdbcStatement;

public abstract class ClientServerWriteBTest extends ClientServerBTest {

    protected int loop = 100;
    protected int sqlCountPerLoop = 500;
    /// protected int threadCount = 20;
    protected boolean async;

    public void start() {
        String name = getBTestName();
        DbType dbType;
        if (name.startsWith("AsyncLealone")) {
            dbType = DbType.Lealone;
            async = true;
        } else if (name.startsWith("Lealone")) {
            dbType = DbType.Lealone;
            async = false;
        } else if (name.startsWith("PgLealone")) {
            dbType = DbType.Lealone;
        } else if (name.startsWith("H2")) {
            dbType = DbType.H2;
        } else if (name.startsWith("MySQL")) {
            dbType = DbType.MySQL;
        } else if (name.startsWith("Pg")) {
            dbType = DbType.PostgreSQL;
        } else {
            throw new RuntimeException("Unsupported BTestName: " + name);
        }
        run(dbType);
    }

    @Override
    public void run() throws Exception {
        init();
        run(threadCount);
    }

    @Override
    protected void run(int threadCount) throws Exception {
        UpdateThreadBase[] threads = new UpdateThreadBase[threadCount];
        for (int i = 0; i < threadCount; i++) {
            Connection conn = getConnection();
            threads[i] = createUpdateThread(i, conn);
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
        System.out.println(getBTestName() + " total time: " + //
                TimeUnit.NANOSECONDS.toMillis(t2 - t1) + " ms");
    }

    protected abstract UpdateThreadBase createUpdateThread(int id, Connection conn);

    protected abstract class UpdateThreadBase extends Thread {

        protected Connection conn;
        protected Statement stmt;

        public UpdateThreadBase(int id, Connection conn) {
            super(getBTestName() + "Thread-" + id);
            this.conn = conn;
            try {
                this.stmt = conn.createStatement();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        protected abstract String nextSql();

        public void warmUp() throws Exception {
        }

        @Override
        public void run() {
            try {
                if (async)
                    executeUpdateAsync(stmt);
                else
                    executeUpdate(stmt);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                close(stmt, conn);
            }
        }

        protected void executeUpdateAsync(Statement statement) throws Exception {
            JdbcStatement stmt = (JdbcStatement) statement;
            for (int j = 0; j < loop; j++) {
                CountDownLatch latch = new CountDownLatch(sqlCountPerLoop);
                long t1 = System.nanoTime();
                for (int i = 0; i < sqlCountPerLoop; i++) {
                    stmt.executeUpdateAsync(nextSql()).onComplete(ar -> {
                        latch.countDown();
                    });
                }
                latch.await();
                long t2 = System.nanoTime();
                System.out.println(getBTestName() + ": "
                        + TimeUnit.NANOSECONDS.toMicros(t2 - t1) / sqlCountPerLoop);
            }
            System.out.println();
            System.out.println("time: 微秒");
            System.out.println("loop: " + loop + " * " + sqlCountPerLoop);
        }

        protected void executeUpdate(Statement statement) throws Exception {
            for (int j = 0; j < loop; j++) {
                long t1 = System.nanoTime();
                for (int i = 0; i < sqlCountPerLoop; i++)
                    statement.executeUpdate(nextSql());
                long t2 = System.nanoTime();
                System.out.println(getBTestName() + ": "
                        + TimeUnit.NANOSECONDS.toMicros(t2 - t1) / sqlCountPerLoop);
            }
            System.out.println();
            System.out.println("time: 微秒");
            System.out.println("loop: " + loop + " * " + sqlCountPerLoop);
        }
    }
}
