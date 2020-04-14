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
package org.lealone.plugins.vertx.net;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetServerBase;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;

public class VertxNetServer extends NetServerBase {

    private static final Logger logger = LoggerFactory.getLogger(VertxNetServer.class);
    private static Vertx vertx;

    private NetServer server;

    @Override
    public void init(Map<String, String> config) { // TODO 对于不支持的参数直接报错
        super.init(config);

        synchronized (VertxNetServer.class) {
            if (vertx == null) {
                vertx = VertxNetUtils.getVertx(config);

                // NetClientOptions options =
                // NetFactory.getNetClientOptions(ConfigDescriptor.getClientEncryptionOptions());
                // options.setConnectTimeout(10000);
                // options.setReconnectAttempts(3);
                // client = vertx.createNetClient(options);
            }
        }
    }

    private NetServer getNetServer() {
        NetServerOptions nso;
        if (ssl) {
            ServerEncryptionOptions options = getServerEncryptionOptions();
            nso = VertxNetUtils.getNetServerOptions(options);
        } else {
            nso = VertxNetUtils.getNetServerOptions(null);
        }
        nso.setHost(host);
        nso.setPort(port);
        NetServer server = vertx.createNetServer(nso);
        return server;
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;

        logger.info("Starting vertx net server...");
        // long t1 = System.currentTimeMillis();
        server = getNetServer();
        // long t2 = System.currentTimeMillis();
        // logger.info("Create vertx net server: " + (t2 - t1) + "ms");

        server.connectHandler(socket -> {
            String host = socket.remoteAddress().host();
            if (VertxNetServer.this.allow(host)) {
                VertxWritableChannel channel = new VertxWritableChannel(socket);
                AsyncConnection conn = VertxNetServer.this.createConnection(channel, true);
                socket.handler(buffer -> {
                    conn.handle(new VertxBuffer(buffer));
                });
                String msg = "RemoteAddress " + socket.remoteAddress() + " ";
                socket.closeHandler(v -> {
                    logger.info(msg + "closed");
                    connectionManager.removeConnection(conn);
                });
                socket.exceptionHandler(v -> {
                    logger.warn(msg + "exception: " + v.getMessage());
                    connectionManager.removeConnection(conn);
                });
            } else {
                // TODO
                // should support a list of allowed databases
                // and a list of allowed clients
                socket.close();
                throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        // t1 = System.currentTimeMillis();
        server.listen(port, host, res -> {
            latch.countDown();
            if (res.succeeded()) {
                // logger.info("Vertx net server listening on port " + server.actualPort());
            } else {
                Throwable e = res.cause();
                checkBindException(e, "Failed to start vertx net server");
            }
        });

        try {
            latch.await();
            super.start();
            // t2 = System.currentTimeMillis();
            // logger.info("Listen completed: " + (t2 - t1) + "ms");
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();

        CountDownLatch latch = new CountDownLatch(1);
        server.close(v -> {
            latch.countDown();
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        }
    }
}
