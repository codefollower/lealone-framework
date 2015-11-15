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
package org.lealone.server.mysql;

import java.io.File;
import java.util.HashMap;

import org.lealone.db.SysProperties;
import org.lealone.server.mysql.MySQLServer;

public class MySQLServerTest {
    public static final String TEST_DIR = "." + File.separatorChar + "lealone-test-data" + File.separatorChar + "test";
    public static final String DB_NAME = "test";

    public static void main(String[] args) {
        SysProperties.setBaseDir(TEST_DIR);
        HashMap<String, String> map = new HashMap<>();
        map.put("listen_port", "3307");
        MySQLServer server = new MySQLServer();
        server.init(map);
        server.start();
        System.out.println(server.getName() + " started, port: " + server.getPort());
    }
}
