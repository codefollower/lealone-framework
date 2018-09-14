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
package org.lealone.plugins.mysql;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.New;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetEndpoint;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetServer;
import org.lealone.net.WritableChannel;
import org.lealone.server.DelegatedProtocolServer;

public class MySQLServer extends DelegatedProtocolServer implements AsyncConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(MySQLServer.class);
    public static final int DEFAULT_PORT = 7214;

    private final Set<MySQLConnection> connections = Collections.synchronizedSet(new HashSet<MySQLConnection>());
    private boolean trace;

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public String getType() {
        return MySQLServerEngine.NAME;
    }

    void trace(String msg) {
        if (trace)
            logger.info(msg);
    }

    void traceError(Throwable e) {
        logger.info(e);
    }

    boolean getTrace() {
        return trace;
    }

    @Override
    public void init(Map<String, String> config) {
        if (!config.containsKey("port"))
            config.put("port", String.valueOf(DEFAULT_PORT));

        trace = Boolean.parseBoolean(config.get("trace"));

        NetFactory factory = NetFactoryManager.getFactory(config);
        NetServer netServer = factory.createNetServer();
        netServer.setConnectionManager(this);
        setProtocolServer(netServer);
        netServer.init(config);

        NetEndpoint.setLocalTcpEndpoint(getHost(), getPort());
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();

        for (MySQLConnection c : New.arrayList(connections)) {
            c.close();
        }
    }

    @Override
    public synchronized AsyncConnection createConnection(WritableChannel writableChannel, boolean isServer) {
        MySQLConnection conn = new MySQLConnection(this, writableChannel, isServer);
        connections.add(conn);
        return conn;
    }

    @Override
    public synchronized void removeConnection(AsyncConnection conn) {
        connections.remove(conn);
    }
}
