/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.vertx;

import java.util.Map;

import org.lealone.plugins.service.http.HttpServerEngine;
import org.lealone.server.ProtocolServer;

public class VertxServerEngine extends HttpServerEngine {

    public static final String NAME = "vertx";
    private VertxServer server; // 延迟初始化

    public VertxServerEngine() {
        super(NAME);
    }

    @Override
    public ProtocolServer getProtocolServer() {
        if (server == null)
            server = new VertxServer();
        return server;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        getProtocolServer().init(config);
    }

    @Override
    public void close() {
        getProtocolServer().stop();
    }
}
