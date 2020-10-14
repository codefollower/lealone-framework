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
package org.lealone.plugins.vertx.server;

import java.util.Map;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.server.ProtocolServerBase;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;

public class HttpServer extends ProtocolServerBase {

    private static final Logger logger = LoggerFactory.getLogger(HttpServer.class);

    public static final int DEFAULT_HTTP_PORT = 8080;

    private String webRoot;
    private String apiPath;
    private Vertx vertx;
    private io.vertx.core.http.HttpServer vertxHttpServer;

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public String getType() {
        return HttpServerEngine.NAME;
    }

    @Override
    public void init(Map<String, String> config) {
        if (!config.containsKey("port"))
            config.put("port", String.valueOf(DEFAULT_HTTP_PORT));

        webRoot = config.get("web_root");
        apiPath = config.get("api_path");
        super.init(config);
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        startVertxHttpServer();
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();

        if (vertxHttpServer != null) {
            vertxHttpServer.close();
            vertxHttpServer = null;
        }
        if (vertx != null) {
            vertx.close();
            vertx = null;
        }
    }

    private void startVertxHttpServer() {
        if (apiPath == null)
            apiPath = "/_lealone_sockjs_/*";
        final String path = apiPath;
        VertxOptions opt = new VertxOptions();
        opt.setBlockedThreadCheckInterval(Integer.MAX_VALUE);
        vertx = Vertx.vertx(opt);
        vertxHttpServer = vertx.createHttpServer();
        Router router = Router.router(vertx);

        String syncRequestUrl = "/_lealone_sync_request_";
        router.post(syncRequestUrl).handler(BodyHandler.create());
        router.post(syncRequestUrl).handler(routingContext -> {
            String command = routingContext.request().params().get("command");
            Buffer result = ServiceHandler.handle(routingContext, command);
            routingContext.request().response().headers().set("Access-Control-Allow-Origin", "*");
            routingContext.request().response().end(result);
        });

        router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST));
        setSockJSHandler(router);
        // 放在最后
        setStaticHandler(router);

        vertxHttpServer.requestHandler(router::handle).listen(port, host, res -> {
            if (res.succeeded()) {
                logger.info("web root: " + webRoot);
                logger.info("sockjs path: " + path);
                logger.info("http server is now listening on port: " + vertxHttpServer.actualPort());
            } else {
                logger.error("failed to bind " + port + " port!", res.cause());
            }
        });
    }

    private void setSockJSHandler(Router router) {
        SockJSHandlerOptions options = new SockJSHandlerOptions().setHeartbeatInterval(2000);
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx, options);
        sockJSHandler.socketHandler(new ServiceHandler(config));
        router.route(apiPath).handler(sockJSHandler);
    }

    private void setStaticHandler(Router router) {
        for (String root : webRoot.split(",", -1)) {
            root = root.trim();
            if (root.isEmpty())
                continue;
            StaticHandler sh = StaticHandler.create(root);
            sh.setCachingEnabled(false);
            router.route("/*").handler(sh);
        }
    }
}
