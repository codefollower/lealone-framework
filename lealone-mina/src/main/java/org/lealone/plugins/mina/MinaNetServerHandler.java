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
package org.lealone.plugins.mina;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnection;

public class MinaNetServerHandler extends IoHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(MinaNetServerHandler.class);
    private static final ConcurrentHashMap<IoSession, AsyncConnection> connections = new ConcurrentHashMap<>();

    private final MinaNetServer server;

    public MinaNetServerHandler(MinaNetServer server) {
        this.server = server;
    }

    @Override
    public void sessionCreated(IoSession session) throws Exception {
        logger.info("Session created, RemoteAddress: " + session.getRemoteAddress());
        MinaWritableChannel writableChannel = new MinaWritableChannel(session);
        AsyncConnection conn = server.createConnection(writableChannel, true);
        connections.put(session, conn);
    }

    @Override
    public void sessionClosed(IoSession session) throws Exception {
        logger.info("Session closed, RemoteAddress: " + session.getRemoteAddress());
        AsyncConnection conn = connections.get(session);
        server.removeConnection(conn);
    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause) {
        session.closeNow();
        String msg = "RemoteAddress " + session.getRemoteAddress();
        logger.error(msg + " exception: " + cause.getMessage(), cause);
        logger.info(msg + " closed");
    }

    @Override
    public void messageReceived(IoSession session, Object message) throws Exception {
        AsyncConnection conn = connections.get(session);
        if (message instanceof IoBuffer) {
            conn.handle(new MinaBuffer((IoBuffer) message));
        } else {
            throw DbException.throwInternalError("message type is " + message.getClass().getName() + " not IoBuffer");
        }
    }
}
