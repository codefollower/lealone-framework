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
package org.lealone.test.perf;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import org.lealone.db.LealoneDatabase;
import org.lealone.plugins.test.mysql.MySQLPreparedStatementTest;
import org.lealone.test.TestBase;

public class PerfTestBase {

    public static final String PERF_TEST_BASE_DIR = "." + File.separatorChar + "target" + File.separatorChar
            + "perf-test-data";

    public static Connection getH2Connection() throws Exception {
        return getH2Connection(false);
    }

    public static Connection getH2Connection(boolean isEmbedded) throws Exception {
        String url;
        if (isEmbedded) {
            url = "jdbc:h2:file:./EmbeddedPerfTestDB";
            // url = "jdbc:h2:mem:mydb";
        } else {
            url = "jdbc:h2:tcp://localhost:9092/CSPerfTestDB";
        }
        return DriverManager.getConnection(url, "sa", "");
    }

    public static Connection getLealoneConnection() throws Exception {
        String url = new TestBase().getURL(LealoneDatabase.NAME);
        return DriverManager.getConnection(url);
    }

    public static Connection getMySqlConnection() throws Exception {
        return MySQLPreparedStatementTest.getMySQLConnection(true);
    }

    protected final int loopCount = 5;

    public void printResult(int loop, String str) {
        System.out.println(this.getClass().getSimpleName() + " loop: " + loop + str);
    }

    public void run() throws Exception {
        for (int i = 1; i <= loopCount; i++) {
            run(i);
        }
    }

    public void run(int loop) throws Exception {
    }
}
