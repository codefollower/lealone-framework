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
package org.lealone.plugins.test.tomcat;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.servlets.DefaultServlet;
import org.lealone.plugins.tomcat.server.TomcatServer;
import org.lealone.test.UnitTestBase;

public class TomcatServerTest extends UnitTestBase {

    public static void main(String[] args) {
        new TomcatServerTest().runTest(true);
    }

    @Override
    public void test() {
        // 启动TomcatServer
        // 在浏览器中打开下面这个URL:
        // http://localhost:8080/
        startTomcatServer();
    }

    private static void startTomcatServer() {
        String baseDir = TEST_BASE_DIR + "/tomcat";
        File rootApp = new File(baseDir, "/webapps/ROOT");
        if (!rootApp.exists())
            rootApp.mkdirs();

        HashMap<String, String> config = new HashMap<>();
        config.put("base_dir", baseDir);
        config.put("context_path", "");
        config.put("doc_base", "/ROOT");

        TomcatServer server = new TomcatServer();
        server.init(config);
        addServlets(server);
        server.start();
    }

    private static void addServlets(TomcatServer server) {
        server.addServlet("myServlet", new HttpServlet() {
            @Override
            protected void service(HttpServletRequest request, HttpServletResponse response)
                    throws ServletException, IOException {
                response.setCharacterEncoding("UTF-8");
                response.setContentType("text/plain");
                response.setHeader("Server", "Embedded Tomcat");
                try (Writer writer = response.getWriter()) {
                    writer.write("Hello, Tomcat! current time: " + new java.util.Date());
                    writer.flush();
                }
            }
        });
        server.addServlet("defaultServlet", new DefaultServlet());
        server.addServletMappingDecoded("/myServlet", "myServlet");
        server.addServletMappingDecoded("/*", "defaultServlet");
    }
}
