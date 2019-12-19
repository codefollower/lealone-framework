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

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.lealone.net.NetBufferFactory;
import org.lealone.net.WritableChannel;

public class MinaWritableChannel implements WritableChannel {

    private final IoSession session;
    private final InetSocketAddress address;

    public MinaWritableChannel(IoSession session) {
        this.session = session;
        SocketAddress sa = session.getRemoteAddress();
        if (sa instanceof InetSocketAddress) {
            address = (InetSocketAddress) sa;
        } else {
            address = null;
        }
    }

    @Override
    public void write(Object data) {
        if (data instanceof MinaBuffer) {
            IoBuffer buffer = ((MinaBuffer) data).getBuffer();
            buffer.flip();
            session.write(buffer);
        } else {
            session.write(data);
        }
    }

    @Override
    public void close() {
        session.closeNow();
    }

    @Override
    public String getHost() {
        return address == null ? "" : address.getHostString();
    }

    @Override
    public int getPort() {
        return address == null ? -1 : address.getPort();
    }

    @Override
    public NetBufferFactory getBufferFactory() {
        return MinaBufferFactory.getInstance();
    }
}
