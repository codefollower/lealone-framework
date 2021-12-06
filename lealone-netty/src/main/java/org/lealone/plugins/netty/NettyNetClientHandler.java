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
package org.lealone.plugins.netty;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class NettyNetClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(NettyNetClientHandler.class);

    private final NettyNetClient nettyNetClient;
    private final AsyncConnectionManager connectionManager;
    private final AsyncConnection conn;

    public NettyNetClientHandler(NettyNetClient nettyNetClient, AsyncConnectionManager connectionManager,
            AsyncConnection conn) {
        this.nettyNetClient = nettyNetClient;
        this.connectionManager = connectionManager;
        this.conn = conn;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf buff = (ByteBuf) msg;
            conn.handle(new NettyBuffer(buff));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
        String msg = "RemoteAddress " + ctx.channel().remoteAddress();
        logger.error(msg + " exception: " + cause.getMessage(), cause);
        logger.info(msg + " closed");
        if (connectionManager != null)
            connectionManager.removeConnection(conn);
        nettyNetClient.removeConnection(conn);
    }
}
