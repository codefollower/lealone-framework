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

import java.sql.SQLException;
import java.util.ArrayList;

public class H2PerfTestServer {

    public static void main(String[] args) throws SQLException {
        setH2Properties();

        ArrayList<String> list = new ArrayList<String>();
        // list.add("-tcp");
        // //list.add("-tool");
        // org.h2.tools.Server.main(list.toArray(new String[list.size()]));
        //
        // list.add("-tcp");
        // list.add("-tcpPort");
        // list.add("9092");

        // 测试org.h2.server.TcpServer.checkKeyAndGetDatabaseName(String)
        // list.add("-key");
        // list.add("mydb");
        // list.add("mydatabase");

        // list.add("-pg");
        list.add("-tcp");
        // list.add("-web");
        // list.add("-ifExists");
        list.add("-ifNotExists");
        list.add("-tcpAllowOthers");
        org.h2.tools.Server.main(list.toArray(new String[list.size()]));
    }

    public static void setH2Properties() {
        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("h2.lobInDatabase", "false");
        System.setProperty("h2.lobClientMaxSizeMemory", "1024");
        System.setProperty("java.io.tmpdir", PerfTestBase.PERF_TEST_BASE_DIR + "/h2/tmp");
        System.setProperty("h2.baseDir", PerfTestBase.PERF_TEST_BASE_DIR + "/h2");
        // System.setProperty("h2.check2", "true");
    }
}
