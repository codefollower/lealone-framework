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
package org.lealone.plugins.test.vertx.server;

import java.util.HashMap;

import org.lealone.plugins.vertx.server.VertxServer;
import org.lealone.test.UnitTestBase;
import org.lealone.test.orm.SqlScript;

public class HttpServerTest extends UnitTestBase {

    public static void main(String[] args) {
        new HttpServerTest().runTest(true);
    }

    @Override
    public void test() {
        // 启动HttpServer
        // 在浏览器中打开下面这个URL:
        // http://localhost:8080/vueTest.html
        startHttpServer();
    }

    static void startHttpServer() {
        HashMap<String, String> config = new HashMap<>();
        config.put("default_database", "test");
        config.put("default_schema", "public");
        config.put("web_root", "../lealone-vertx/src/main/resources/js,../lealone-vertx/src/test/resources");
        VertxServer server = new VertxServer();
        server.init(config);
        server.start();
    }

    static void setCodePath() {
        SqlScript.setCodePath("../../lealone-database/lealone-test/src/test/java");
    }
}
