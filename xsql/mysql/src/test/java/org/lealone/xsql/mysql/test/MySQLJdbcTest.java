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
package org.lealone.xsql.mysql.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.lealone.xsql.mysql.server.MySQLServer;

public class MySQLJdbcTest {

    public static Connection getMySQLConnection() throws Exception {
        return getMySQLConnection(true, MySQLServer.DEFAULT_PORT);
    }

    public static Connection getMySQLConnection(boolean autoCommit, int port) throws Exception {
        // String driver = "com.mysql.jdbc.Driver";
        // Class.forName(driver);

        String db = "mysql";
        String password = "";
        // db = "test";
        // password = "zhh";

        String url = "jdbc:mysql://localhost:" + port + "/" + db;

        Properties info = new Properties();
        info.put("user", "root");
        info.put("password", password);
        // info.put("holdResultsOpenOverStatementClose","true");
        // info.put("allowMultiQueries","true");

        // info.put("useServerPrepStmts", "true");
        // info.put("cachePrepStmts", "true");
        info.put("rewriteBatchedStatements", "true");
        info.put("useCompression", "true");
        info.put("serverTimezone", "GMT");

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

    public static void main(String[] args) throws Exception {
        Connection conn = getMySQLConnection();
        crud(conn);
    }

    public static void crud(Connection conn) {
        try {
            Statement statement = conn.createStatement();
            statement.executeUpdate("drop table if exists pet");
            statement.executeUpdate("create table if not exists pet(name varchar(20), age int)");
            statement.executeUpdate("insert into pet values('pet1', 2)");

            ResultSet rs = statement.executeQuery("select count(*) from pet");
            rs.next();
            System.out.println("Statement.executeQuery count: " + rs.getInt(1));
            rs.close();
            statement.close();

            String sql = "select name, age from pet where name=?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, "pet1");
            rs = ps.executeQuery();
            rs.next();
            String name = rs.getString(1);
            int age = rs.getInt(2);
            System.out.println("PreparedStatement.executeQuery name: " + name + ", age: " + age);
            rs.close();
            ps.close();

            conn.close();
        } catch (SQLException e) {
            sqlException(e);
            e.printStackTrace();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
