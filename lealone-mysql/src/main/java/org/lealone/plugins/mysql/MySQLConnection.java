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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetBuffer;
import org.lealone.net.WritableChannel;
import org.lealone.plugins.mysql.protocol.AuthPacket;
import org.lealone.plugins.mysql.protocol.Command;
import org.lealone.plugins.mysql.protocol.HandshakePacket;
import org.lealone.plugins.mysql.protocol.OkPacket;

//TODO 还有很多要实现
public class MySQLConnection extends AsyncConnection {

    private static final Logger logger = LoggerFactory.getLogger(MySQLConnection.class);

    // private static final int BUFFER_SIZE = 16 * 1024;

    // private final MySQLServer server;

    private DataInputStream in;
    private DataOutputStream out;

    protected MySQLConnection(MySQLServer server, WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
        // this.server = server;
    }

    // private void trace(String s) {
    // server.trace(this + " " + s);
    // }

    private final byte[] packetHeaderBuf = new byte[4];

    private byte[] readPacket(AuthPacket ap) throws IOException {
        readFully(in, packetHeaderBuf, 0, 4);
        int packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8)
                + ((this.packetHeaderBuf[2] & 0xff) << 16);

        ap.packetId = (byte) (packetHeaderBuf[3] & 0xff);

        // int packetSequence = packetHeaderBuf[3] & 0xff;
        byte[] buffer = new byte[packetLength];
        int numBytesRead = readFully(in, buffer, 0, packetLength);
        if (numBytesRead != packetLength) {
            throw new IOException("Short read, expected " + packetLength + " bytes, only read " + numBytesRead);
        }
        return buffer;
    }

    private byte[] readPacket() throws IOException {
        readFully(in, packetHeaderBuf, 0, 4);
        int packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8)
                + ((this.packetHeaderBuf[2] & 0xff) << 16);

        // int packetSequence = packetHeaderBuf[3] & 0xff;
        byte[] buffer = new byte[packetLength];
        int numBytesRead = readFully(in, buffer, 0, packetLength);
        if (numBytesRead != packetLength) {
            throw new IOException("Short read, expected " + packetLength + " bytes, only read " + numBytesRead);
        }
        return buffer;
    }

    private final int readFully(InputStream in, byte[] b, int off, int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len) {
            int count = in.read(b, off + n, len - n);

            if (count < 0) {
                throw new EOFException();
            }

            n += count;
        }

        return n;
    }

    void Handshake() throws IOException {
        int threadId = 0;
        HandshakePacket.create(threadId).write(out);

        AuthPacket ap = new AuthPacket();
        byte[] packet = readPacket(ap);
        ap.read(packet);

        OkPacket ok = new OkPacket();
        ok.packetId = ap.packetId;
        ok.write(out);
    }

    private static class BufferInputStream extends InputStream {
        final NetBuffer buffer;
        final int size;
        int pos;

        BufferInputStream(NetBuffer buffer) {
            this.buffer = buffer;
            size = buffer.length();
        }

        @Override
        public int available() throws IOException {
            return size - pos;
        }

        @Override
        public int read() throws IOException {
            return buffer.getUnsignedByte(pos++);
        }
    }

    private static class BufferOutputStream extends OutputStream {
        final WritableChannel writableChannel;
        final int initialSizeHint;
        NetBuffer buffer;

        BufferOutputStream(WritableChannel writableChannel, int initialSizeHint) {
            this.writableChannel = writableChannel;
            this.initialSizeHint = initialSizeHint;
            reset();
        }

        @Override
        public void write(int b) {
            buffer.appendByte((byte) b);
        }

        @Override
        public void write(byte b[], int off, int len) {
            buffer.appendBytes(b, off, len);
        }

        void reset() {
            buffer = writableChannel.getBufferFactory().createBuffer(initialSizeHint);
        }

        @Override
        public void flush() throws IOException {
            super.flush();
            writableChannel.write(buffer);
            reset();
        }
    }

    private NetBuffer lastBuffer;

    @Override
    public void handle(NetBuffer buffer) {
        if (lastBuffer != null) {
            buffer = lastBuffer.appendBuffer(buffer);
            lastBuffer = null;
        }
        int length = buffer.length();
        if (length < 1) {
            return;
        }

        out = new DataOutputStream(new BufferOutputStream(writableChannel, 1024));
        in = new DataInputStream(new BufferInputStream(buffer));
        try {
            parsePacket();
        } catch (Throwable e) {
            logger.error("Parse packet exception", e);
        }
    }

    private void parsePacket() throws IOException {
        byte[] packet = readPacket();
        int operation = packet[0] & 0xff;
        switch (operation) {
        case Command.QUERY:
            // String sql = new String(packet, 1, packet.length - 1);
            break;
        default:
        }
    }
}
