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
package org.lealone.plugins.test.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.lealone.plugins.postgresql.PgServer;

public class PgStatementTest {

    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://localhost:" + PgServer.DEFAULT_PORT + "/pg_test";
        Properties info = new Properties();
        info.put("user", "test");
        info.put("password", "test");

        Connection conn = DriverManager.getConnection(url, info);
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table if exists pet");
        statement.executeUpdate("create table if not exists pet(name varchar(20), age int)");
        statement.executeUpdate("insert into pet values('pet1', 2)");
        statement.close();
        ResultSet rs = statement.executeQuery("select count(*) from pet");
        rs.next();
        System.out.println("count: " + rs.getInt(1));
        rs.close();
        statement.close();
        conn.close();
    }
}
