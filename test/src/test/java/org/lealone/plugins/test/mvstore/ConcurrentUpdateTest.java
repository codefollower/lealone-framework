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
package org.lealone.plugins.test.mvstore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.jdbc.JdbcStatement;

public class ConcurrentUpdateTest {
    public static Connection getConnection(boolean isEmbedded) throws Exception {
        String url;
        if (isEmbedded) {
            url = "jdbc:h2:file:./EmbeddedPerfTestDB";
            // url = "jdbc:h2:mem:mydb";
        } else {
            url = "jdbc:h2:tcp://localhost:9092/CSPerfTestDB";
        }
        // url += ";OPEN_NEW=true;FORBID_CREATION=false";
        url += ";FORBID_CREATION=false";
        return DriverManager.getConnection(url, "sa", "");
    }

    public static void main(String[] args) throws Exception {
        Connection conn = getConnection(false);
        JdbcStatement stmt = (JdbcStatement) conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS ConcurrentUpdateTest");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ConcurrentUpdateTest (f1 int primary key, f2 long)");
        String sql = "INSERT INTO ConcurrentUpdateTest(f1, f2) VALUES(1, 2)";
        stmt.executeUpdate(sql);
        sql = "INSERT INTO ConcurrentUpdateTest(f1, f2) VALUES(2, 2)";
        stmt.executeUpdate(sql);
        sql = "INSERT INTO ConcurrentUpdateTest(f1, f2) VALUES(3, 2)";
        stmt.executeUpdate(sql);
        sql = "INSERT INTO ConcurrentUpdateTest(f1, f2) VALUES(4, 2)";
        stmt.executeUpdate(sql);

        int threadsCount = 2;
        UpdateThread[] updateThreads = new UpdateThread[threadsCount];
        DeleteThread[] deleteThreads = new DeleteThread[threadsCount];
        QueryThread[] queryThreads = new QueryThread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            updateThreads[i] = new UpdateThread(i);
        }
        for (int i = 0; i < threadsCount; i++) {
            deleteThreads[i] = new DeleteThread(i);
        }
        for (int i = 0; i < threadsCount; i++) {
            queryThreads[i] = new QueryThread(i);
        }

        for (int i = 0; i < threadsCount; i++) {
            updateThreads[i].start();
            deleteThreads[i].start();
            queryThreads[i].start();
        }
        for (int i = 0; i < threadsCount; i++) {
            updateThreads[i].join();
            deleteThreads[i].join();
            queryThreads[i].join();
        }
        ConcurrentUpdateTest.close(stmt, conn);
    }

    static class UpdateThread extends Thread {
        JdbcStatement stmt;
        Connection conn;

        UpdateThread(int id) throws Exception {
            super("UpdateThread-" + id);
            conn = getConnection(false);
            stmt = (JdbcStatement) conn.createStatement();
        }

        @Override
        public void run() {
            try {
                conn.setAutoCommit(false);

                String sql = "update ConcurrentUpdateTest set f2=3 where f1>=1";
                stmt.executeUpdate(sql);

                conn.commit();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ConcurrentUpdateTest.close(stmt, conn);
            }
        }
    }

    static class DeleteThread extends Thread {
        JdbcStatement stmt;
        Connection conn;

        DeleteThread(int id) throws Exception {
            super("DeleteThread-" + id);
            conn = getConnection(false);
            stmt = (JdbcStatement) conn.createStatement();
        }

        @Override
        public void run() {
            try {
                conn.setAutoCommit(false);

                String sql = "delete from ConcurrentUpdateTest where f1=4";
                stmt.executeUpdate(sql);

                conn.commit();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ConcurrentUpdateTest.close(stmt, conn);
            }
        }
    }

    static class QueryThread extends Thread {
        JdbcStatement stmt;
        Connection conn;

        QueryThread(int id) throws Exception {
            super("QueryThread-" + id);
            conn = getConnection(false);
            stmt = (JdbcStatement) conn.createStatement();
        }

        @Override
        public void run() {
            try {
                ResultSet rs = stmt.executeQuery("SELECT * FROM ConcurrentUpdateTest where f1 = 1");
                while (rs.next()) {
                    System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ConcurrentUpdateTest.close(stmt, conn);
            }
        }
    }

    static void close(JdbcStatement stmt, Connection conn) {
        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
