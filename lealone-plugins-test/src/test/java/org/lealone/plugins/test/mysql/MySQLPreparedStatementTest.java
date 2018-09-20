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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.lealone.plugins.mysql.server.MySQLServer;

public class MySQLPreparedStatementTest {
    private static Connection getMySQLConnection() throws Exception {
        return getMySQLConnection(true);
    }

    private static Connection getMySQLConnection(boolean autoCommit) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        int port = MySQLServer.DEFAULT_PORT;
        String db = "mysql";
        String password = "";

        // port = 8077;
        // db = "dbtest";
        // password = "zhh";

        port = 3306;
        db = "test";
        password = "zhh";

        String url = "jdbc:mysql://localhost:" + port + "/" + db;

        Properties info = new Properties();
        info.put("user", "root");
        info.put("password", password);
        // info.put("holdResultsOpenOverStatementClose","true");
        // info.put("allowMultiQueries","true");

        // info.put("useServerPrepStmts", "true");

        Class.forName(driver);

        Connection conn = DriverManager.getConnection(url, info);
        conn.setAutoCommit(autoCommit);
        return conn;
    }

    public static void sqlException(SQLException e) {
        while (e != null) {
            System.err.println("SQLException:" + e);
            System.err.println("-----------------------------------");
            System.err.println("Message  : " + e.getMessage());
            System.err.println("SQLState : " + e.getSQLState());
            System.err.println("ErrorCode: " + e.getErrorCode());
            System.err.println();
            System.err.println();
            e = e.getNextException();
        }
    }

    static void print(int[] array) {
        if (array == null)
            System.err.println("array=null");

        System.err.println("array.length=" + array.length);
        for (int i : array) {
            System.err.println("i=" + i);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            Connection conn = getMySQLConnection();
            Statement statement = conn.createStatement();
            statement.executeUpdate("drop table if exists pet");
            statement.executeUpdate("create table if not exists pet(name varchar(20), age int)");
            statement.executeUpdate("insert into pet values('pet1', 2)");
            statement.close();
            // conn.close();
            // String sql = null;
            // String sql = "select * from pet where name = ? ON DUPLICATE KEY UPDATE ";
            String sql = "select * from pet where age=? or name=?";
            PreparedStatement stmt = conn.prepareStatement(sql);
            System.err.println(stmt.getClass().getName());

            long t1 = System.currentTimeMillis();
            for (int i = 0; i < 2000; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, i + "");
                stmt.executeQuery();
            }

            System.err.println(System.currentTimeMillis() - t1);

            stmt.close();
            conn.close();
        } catch (SQLException e) {
            sqlException(e);
            e.printStackTrace();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}