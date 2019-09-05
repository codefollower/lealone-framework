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
package org.lealone.plugins.test.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

//因为update语句会转成removeRow和addRow操作，所以不会跟delete语句产生死锁
public class ReadWriteLockTest {
    static Connection getConnection(String url) throws Exception {
        // Properties prop = new Properties();
        // prop.setProperty("MULTI_THREADED", "true");
        // return DriverManager.getConnection(url, prop);

        return MySQLPreparedStatementTest.getMySQLConnection(true);
    }

    static String url = "jdbc:h2:mem:mydb";

    public static void main(String[] args) throws Exception {
        // updateRow();

        // repeatableRead();
        // indexTest();

        rowLockTest();
    }

    public static void updateRow() throws Exception {
        Connection conn = getConnection(url);
        // conn.setAutoCommit(true);
        System.out.println("isAutoCommit: " + conn.getAutoCommit());
        createTable(conn);
        System.out.println("isAutoCommit: " + conn.getAutoCommit());
        Statement stmt = conn.createStatement();
        int count = 5000;
        stmt.executeUpdate("INSERT INTO DeadLockTest(f1, f2, f3) VALUES(100, " + count + ", 30)");
        // stmt.executeUpdate("UPDATE DeadLockTest SET f2=f2-1 where f1=100 and f2>0");
        // conn.commit();

        int n = 100;
        int loop = count / n;
        AtomicLong total = new AtomicLong();
        Thread[] threads = new Thread[n];
        for (int i = 0; i < n; i++) {
            threads[i] = new Thread(() -> {
                try {
                    Connection conn2 = getConnection(url);
                    // conn2.setAutoCommit(false);
                    // conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    Statement stmt2 = conn2.createStatement();
                    long t1 = System.currentTimeMillis();
                    for (int j = 0; j < loop; j++) {
                        stmt2.executeUpdate("UPDATE DeadLockTest SET f2=f2-1 where f1=100 and f2>0");
                    }
                    total.addAndGet(System.currentTimeMillis() - t1);
                    // conn2.commit();
                    conn2.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        for (int i = 0; i < n; i++) {
            threads[i].start();
        }
        for (int i = 0; i < n; i++) {
            threads[i].join();
        }
        conn.close();
        System.out.println("count: " + count + ", threads: " + n + ", avg time: " + total.get() / n + " ms");
    }

    public static void createTable(Connection conn) throws Exception {
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS DeadLockTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS DeadLockTest (f1 int primary key, f2 int, f3 int)");
        stmt.executeUpdate("CREATE INDEX   DeadLockIndexTest1 ON DeadLockTest(f2)");
        stmt.executeUpdate("CREATE INDEX   DeadLockIndexTest2 ON DeadLockTest(f3)");
        stmt.executeUpdate("INSERT INTO DeadLockTest(f1, f2, f3) VALUES(10, 20, 30)");
        stmt.executeUpdate("DROP TABLE IF EXISTS DeadLockTest2");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS DeadLockTest2 (f1 int primary key, f2 int, f3 int)");
        stmt.executeUpdate("INSERT INTO DeadLockTest2(f1, f2, f3) VALUES(10, 20, 30)");
    }

    public static void repeatableRead() throws Exception {
        Connection conn = getConnection(url);
        createTable(conn);

        new Thread(() -> {
            try {
                Connection conn2 = getConnection(url);
                conn2.setAutoCommit(false);
                conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                Statement stmt2 = conn2.createStatement();
                // stmt2.executeUpdate("INSERT INTO DeadLockTest(f1, f2, f3) VALUES(20, 20, 30)");
                stmt2.executeUpdate("delete from DeadLockTest where f1=10");
                conn2.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                Connection conn1 = getConnection(url);
                conn1.setAutoCommit(false);
                conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

                // conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                Statement stmt1 = conn1.createStatement();
                ResultSet rs = stmt1.executeQuery("select * from DeadLockTest where f2=20");
                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                }
                rs.close();
                rs = stmt1.executeQuery("select * from DeadLockTest where f3=30");
                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                } else {
                    System.out.println("empty");
                }
                rs.close();

                // rs = stmt1.executeQuery("select count(*) from DeadLockTest where f3>=30 lock in share mode");
                // rs = stmt1.executeQuery("select count(*) from DeadLockTest where f3>=30 for update");
                rs = stmt1.executeQuery("select count(*) from DeadLockTest where f3>=30");
                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                } else {
                    System.out.println("empty");
                }
                rs.close();
                rs = stmt1.executeQuery("select count(*) from DeadLockTest where f3>=30");
                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                } else {
                    System.out.println("empty");
                }
                rs.close();
                conn1.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        conn.close();
    }

    public static void indexTest() throws Exception {
        Connection conn = getConnection(url);
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS DeadLockTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS DeadLockTest (f1 int primary key, f2 long)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS DeadLockIndexTest ON DeadLockTest(f2)");
        stmt.executeUpdate("INSERT INTO DeadLockTest(f1, f2) VALUES(10, 12)");

        new Thread(() -> {
            try {
                Connection conn1 = getConnection(url);
                Statement stmt1 = conn1.createStatement();
                stmt1.executeUpdate("UPDATE DeadLockTest SET f2=13 where f1=10");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                Connection conn2 = getConnection(url);
                Statement stmt2 = conn2.createStatement();
                stmt2.executeUpdate("delete from DeadLockTest where f1=10");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        conn.close();
    }

    public static void rowLockTest() throws Exception {
        Connection conn = getConnection(url);
        createTable(conn);

        new Thread(() -> {
            try {
                Connection conn1 = getConnection(url);
                conn1.setAutoCommit(false);
                Statement stmt1 = conn1.createStatement();
                stmt1.executeUpdate("UPDATE DeadLockTest SET f2=f2-2 where f1=10");
                ResultSet rs = stmt1.executeQuery("select f2 from DeadLockTest where f1=10");
                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                }
                rs.close();
                conn1.commit();
                conn1.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                Connection conn1 = getConnection(url);
                conn1.setAutoCommit(false);
                conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

                // conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                Statement stmt1 = conn1.createStatement();
                ResultSet rs = stmt1.executeQuery("select f2 from DeadLockTest where f1=10 for update");
                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                }
                rs.close();
                stmt1.executeUpdate("UPDATE DeadLockTest SET f2=f2-4 where f1=10");
                rs = stmt1.executeQuery("select f2 from DeadLockTest where f1=10");
                if (rs.next()) {
                    System.out.println(rs.getInt(1));
                }
                rs.close();
                conn1.commit();
                conn1.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        conn.close();
    }
}
