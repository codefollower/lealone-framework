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
package ${packageName}.main;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class ${appClassName}SqlScript {

    public static void main(String[] args) throws Exception {
        new ${appClassName}SqlScript().run(args);
    }
 
    private String tableDir = "sql";
    private String serviceDir = "sql";
    private String srcDir = "src/main/java";
    private String jdbcUrl;

    private void run(String[] args) throws Exception {
        parseArgs(args);

        // 创建${dbName}数据库
        jdbcUrl = "jdbc:lealone:tcp://localhost/lealone?user=root&password=";
        // runSql("drop database if exists hello");
        runSql("create database if not exists hello");

        jdbcUrl = "jdbc:lealone:tcp://localhost/${dbName}?user=root&password=";

        // 执行建表脚本，同时自动生成对应的模型类的代码
        runScript(getSqlFile(tableDir, "tables.sql"));

        // 初始化数据
        runScript(getSqlFile(tableDir, "init-data.sql"));

        // 执行服务创建脚本，同时自动生成对应的服务接口代码
        runScript(getSqlFile(serviceDir, "services.sql"));
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            args[i] = args[i].trim();
        }

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
            case "-tableDir":
                tableDir = args[++i];
                break;
            case "-serviceDir":
                serviceDir = args[++i];
                break;
            case "-srcDir":
                srcDir = args[++i];
                break;
            default:
                System.out.println("选项名 '" + a + "' 无效");
                System.exit(-1);
            }
        }
    }

    private String getSqlFile(String dir, String name) {
        return new File(dir, name).getAbsolutePath();
    }

    private void runScript(String scriptFile) throws Exception {
        runSql("RUNSCRIPT FROM '" + scriptFile + "'");
    }

    private void runSql(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("set @srcDir '" + srcDir + "'");
            stmt.executeUpdate(sql);
            System.out.println("execute sql: " + sql);
        }
    }
}
