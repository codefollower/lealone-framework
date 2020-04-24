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
package org.lealone.plugins.tomcat.server;

import java.util.Map;

import javax.servlet.Servlet;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.server.ProtocolServerBase;

public class TomcatServer extends ProtocolServerBase {

    private static final Logger logger = LoggerFactory.getLogger(TomcatServer.class);

    public static final int DEFAULT_HTTP_PORT = 8080;

    private String contextPath;
    private String docBase;
    private Tomcat tomcat;
    private Context ctx;

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public String getType() {
        return TomcatServerEngine.NAME;
    }

    @Override
    public void init(Map<String, String> config) {
        if (!config.containsKey("port"))
            config.put("port", String.valueOf(DEFAULT_HTTP_PORT));
        super.init(config);

        contextPath = config.get("context_path");
        docBase = config.get("doc_base");

        tomcat = new Tomcat();
        tomcat.setBaseDir(baseDir);
        tomcat.setHostname(host);
        tomcat.setPort(port);
        try {
            tomcat.init();
        } catch (LifecycleException e) {
            logger.error("Failed to init tomcat", e);
        }
        ctx = tomcat.addContext(contextPath, docBase);
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        try {
            startTomcat();
            super.start();
        } catch (LifecycleException e) {
            logger.error("Failed to start tomcat", e);
        }
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();

        if (tomcat != null) {
            try {
                tomcat.stop();
            } catch (LifecycleException e) {
                logger.error("Failed to stop tomcat", e);
            }
            tomcat = null;
        }
    }

    private void startTomcat() throws LifecycleException {
        tomcat.getConnector();
        tomcat.start();
        logger.info("Tomcat server is now listening on port: " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                tomcat.destroy();
            } catch (LifecycleException e) {
                logger.error("Failed to destroy tomcat", e);
            }
        }));
    }

    public void addServlet(String servletName, Servlet servlet) {
        Tomcat.addServlet(ctx, servletName, servlet);
    }

    public void addServletMappingDecoded(String pattern, String name) {
        ctx.addServletMappingDecoded(pattern, name);
    }
}
