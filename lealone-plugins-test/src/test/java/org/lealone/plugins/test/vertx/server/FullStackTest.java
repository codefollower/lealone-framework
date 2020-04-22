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

import org.lealone.plugins.test.SqlScript;
import org.lealone.plugins.test.service.ServiceConsumerTest;
import org.lealone.plugins.test.service.ServiceProviderTest;
import org.lealone.test.UnitTestBase;

public class FullStackTest extends UnitTestBase {

    public static void main(String[] args) {
        new FullStackTest().runTest(true, false);
    }

    @Override
    public void test() {
        // 设置jdbc url(可选)
        setJdbcUrl();

        // 创建user表
        SqlScript.createUserTable(this);

        // 创建服务
        ServiceProviderTest.createService(this);

        // 从后端调用服务
        ServiceConsumerTest.callService(getURL());

        // 启动HttpServer
        // 在浏览器中打开下面这个URL，测试从前端发起服务调用，在console里面看结果:
        // http://localhost:8080/fullStackTest.html
        HttpServerTest.startHttpServer();
    }

    private void setJdbcUrl() {
        String url = getURL();
        System.setProperty("lealone.jdbc.url", url);
        System.out.println("jdbc url: " + url);
    }
}
