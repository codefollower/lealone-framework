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
package org.lealone.plugins.vertx;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.DbException;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetEndpoint;
import org.lealone.net.TcpClientConnection;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;

public class VertxNetClient extends NetClientBase {

    private static Vertx vertx;
    private static NetClient vertxClient;

    private static synchronized void openVertxClient(Map<String, String> config) {
        if (vertxClient == null) {
            vertx = VertxNetUtils.getVertx(config);
            NetClientOptions options = VertxNetUtils.getNetClientOptions(config);
            options.setConnectTimeout(10000);
            vertxClient = vertx.createNetClient(options);
        }
    }

    private static synchronized void closeVertxClient() {
        if (vertxClient != null) {
            vertxClient.close();
            VertxNetUtils.closeVertx(vertx); // 不要像这样单独调用: vertx.close();
            vertxClient = null;
            vertx = null;
        }
    }

    @Override
    protected void openInternal(Map<String, String> config) {
        openVertxClient(config);
    }

    @Override
    protected void closeInternal() {
        closeVertxClient();
    }

    @Override
    protected void createConnectionInternal(NetEndpoint endpoint, AsyncConnectionManager connectionManager,
            CountDownLatch latch) throws Exception {
        InetSocketAddress inetSocketAddress = endpoint.getInetSocketAddress();
        vertxClient.connect(endpoint.getPort(), endpoint.getHost(), res -> {
            try {
                if (res.succeeded()) {
                    NetSocket socket = res.result();
                    VertxWritableChannel channel = new VertxWritableChannel(socket);
                    AsyncConnection conn;
                    if (connectionManager != null) {
                        conn = connectionManager.createConnection(channel, false);
                    } else {
                        conn = new TcpClientConnection(channel, this);
                    }
                    conn.setInetSocketAddress(inetSocketAddress);
                    addConnection(inetSocketAddress, conn);
                    socket.handler(buffer -> {
                        conn.handle(new VertxBuffer(buffer));
                    });
                } else {
                    throw DbException.convert(res.cause());
                }
            } finally {
                latch.countDown();
            }
        });
    }
}
