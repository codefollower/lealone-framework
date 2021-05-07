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
package org.lealone.plugins.mysql.server;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.result.Result;
import org.lealone.db.session.Session;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetBuffer;
import org.lealone.net.NetBufferInputStream;
import org.lealone.net.NetBufferOutputStream;
import org.lealone.net.WritableChannel;
import org.lealone.plugins.mysql.server.handler.AuthPacketHandler;
import org.lealone.plugins.mysql.server.handler.CommandPacketHandler;
import org.lealone.plugins.mysql.server.handler.PacketHandler;
import org.lealone.plugins.mysql.server.protocol.AuthPacket;
import org.lealone.plugins.mysql.server.protocol.EOFPacket;
import org.lealone.plugins.mysql.server.protocol.ErrorPacket;
import org.lealone.plugins.mysql.server.protocol.FieldPacket;
import org.lealone.plugins.mysql.server.protocol.HandshakePacket;
import org.lealone.plugins.mysql.server.protocol.OkPacket;
import org.lealone.plugins.mysql.server.protocol.Packet;
import org.lealone.plugins.mysql.server.protocol.PacketInput;
import org.lealone.plugins.mysql.server.protocol.PacketOutput;
import org.lealone.plugins.mysql.server.protocol.ResultSetHeaderPacket;
import org.lealone.plugins.mysql.server.protocol.RowDataPacket;
import org.lealone.plugins.mysql.util.PacketUtil;
import org.lealone.sql.PreparedSQLStatement;

public class MySQLServerConnection extends AsyncConnection {

    private static final Logger logger = LoggerFactory.getLogger(MySQLServerConnection.class);
    private static final int BUFFER_SIZE = 16 * 1024;
    private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };

    private final MySQLServer server;
    private Session session;
    private PacketHandler packetHandler;
    private NetBuffer lastBuffer;

    // private DataInputStream in;
    // private DataOutputStream out;
    // private boolean initialized;

    protected MySQLServerConnection(MySQLServer server, WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
        this.server = server;
    }

    void handshake() {
        DataOutputStream out = new DataOutputStream(new NetBufferOutputStream(writableChannel, BUFFER_SIZE));
        int threadId = 0;

        PacketOutputImpl output = new PacketOutputImpl(out);
        HandshakePacket.create(threadId).write(output);
        packetHandler = new AuthPacketHandler(this);
    }

    static class PacketInputImpl implements PacketInput {
        byte[] data;

        PacketInputImpl(byte[] data) {
            this.data = data;
        }

        @Override
        public byte[] getData() {
            return data;
        }
    }

    static class PacketOutputImpl implements PacketOutput {
        DataOutputStream out;

        PacketOutputImpl(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity) {
            if (capacity > buffer.remaining()) {
                write(buffer);
                return allocate();
            } else {
                return buffer;
            }
        }

        @Override
        public ByteBuffer allocate() {
            return ByteBuffer.allocate(BUFFER_SIZE);
        }

        @Override
        public int getPacketHeaderSize() {
            return Packet.PACKET_HEADER_SIZE;
        }

        @Override
        public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
            int offset = 0;
            int length = src.length;
            int remaining = buffer.remaining();
            while (length > 0) {
                if (remaining >= length) {
                    buffer.put(src, offset, length);
                    break;
                } else {
                    buffer.put(src, offset, remaining);
                    write(buffer);
                    buffer = allocate();
                    offset += remaining;
                    length -= remaining;
                    remaining = buffer.remaining();
                    continue;
                }
            }
            return buffer;
        }

        @Override
        public void write(ByteBuffer buffer) {
            buffer.flip();
            try {
                out.write(buffer.array(), buffer.arrayOffset(), buffer.limit());
                out.flush();
            } catch (IOException e) {
                logger.error("Failed to write", e);
            }
        }

    }

    public void authenticate(AuthPacket authPacket) {
        try {
            session = createSession(authPacket);
        } catch (Throwable e) {
            logger.error("Failed to create session", e);
            sendErrorMessage(e);
            close();
            return;
        }
        server.addConnection(this);
        packetHandler = new CommandPacketHandler(this);
        sendMessage(AUTH_OK);
    }

    private Session createSession(AuthPacket authPacket) {
        Properties info = new Properties();
        info.put("MODE", "MySQL");
        info.put("USER", authPacket.user);
        info.put("PASSWORD", getPassword(authPacket));
        String url = Constants.URL_PREFIX + Constants.URL_EMBED + MySQLServer.DATABASE_NAME;
        ConnectionInfo ci = new ConnectionInfo(url, info);
        return ci.createSession();
    }

    private static String getPassword(AuthPacket authPacket) {
        if (authPacket.password == null || authPacket.password.length == 0)
            return "";
        // TODO MySQL的密码跟Lealone不一样
        return "";
    }

    public void executeStatement(String sql) {
        logger.info("sql: " + sql);
        try {
            PreparedSQLStatement ps = (PreparedSQLStatement) session.prepareSQLCommand(sql, -1);

            if (ps.isQuery()) {
                Result result = ps.executeQuery(-1).get();
                writeQueryResult(result);
            } else {
                int updateCount = ps.executeUpdate().get();
                writeUpdateResult(updateCount);
            }
        } catch (Throwable e) {
            logger.error("Failed to execute statement: " + sql, e);
            sendErrorMessage(e);
            return;
        }
    }

    private void writeQueryResult(Result result) {
        PacketOutput out = getPacketOutput();
        int fieldCount = result.getVisibleColumnCount();
        ResultSetHeaderPacket header = PacketUtil.getHeader(fieldCount);
        FieldPacket[] fields = new FieldPacket[fieldCount];
        EOFPacket eof = new EOFPacket();
        byte packetId = 0;
        header.packetId = ++packetId;
        // packetId++;
        for (int i = 0; i < fieldCount; i++) {
            fields[i] = PacketUtil.getField(result.getColumnName(i), result.getColumnType(i));
            fields[i].packetId = ++packetId;
        }
        eof.packetId = ++packetId;

        ByteBuffer buffer = out.allocate();

        // write header
        buffer = header.write(buffer, out);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, out);
        }

        // write eof
        buffer = eof.write(buffer, out);

        // write rows
        packetId = eof.packetId;
        for (int i = 0; i < result.getRowCount(); i++) {
            RowDataPacket row = new RowDataPacket(fieldCount);
            if (result.next()) {
                Value[] values = result.currentRow();
                for (int j = 0; j < fieldCount; j++) {
                    if (values[j] == ValueNull.INSTANCE) {
                        row.add(new byte[0]);
                    } else {
                        row.add(values[j].toString().getBytes());
                    }
                }
                row.packetId = ++packetId;
                buffer = row.write(buffer, out);
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, out);

        // post write
        out.write(buffer);
    }

    private void writeUpdateResult(int updateCount) {
        PacketOutput out = getPacketOutput();
        OkPacket packet = new OkPacket();
        packet.packetId = 1;
        packet.affectedRows = updateCount;
        packet.serverStatus = 2;
        packet.write(out);
    }

    private final static byte[] encodeString(String src, String charset) {
        if (src == null) {
            return null;
        }
        if (charset == null) {
            return src.getBytes();
        }
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    public void sendErrorMessage(Throwable e) {
        if (e instanceof DbException) {
            DbException dbe = (DbException) e;

            sendErrorMessage(dbe.getErrorCode(), dbe.getMessage());
        } else {
            sendErrorMessage(DbException.convert(e));
        }
    }

    public void sendErrorMessage(int errno, Throwable e) {
        sendErrorMessage(errno, e.getCause());
    }

    public void sendErrorMessage(int errno, String msg) {
        ErrorPacket err = new ErrorPacket();
        err.packetId = 0;
        err.errno = errno;
        err.message = encodeString(msg, "utf-8");
        err.write(getPacketOutput());
    }

    PacketOutput getPacketOutput() {
        DataOutputStream out = new DataOutputStream(new NetBufferOutputStream(writableChannel, BUFFER_SIZE));
        PacketOutputImpl output = new PacketOutputImpl(out);
        return output;
    }

    public void sendMessage(byte[] data) {
        NetBufferOutputStream out = new NetBufferOutputStream(writableChannel, BUFFER_SIZE);
        try {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            logger.error("Failed to send message", e);
        }
    }

    private int getPacketLength(NetBuffer buffer, int pos) throws IOException {
        int length = buffer.getUnsignedByte(pos) & 0xff;
        length |= (buffer.getUnsignedByte(pos + 1) & 0xff) << 8;
        length |= (buffer.getUnsignedByte(pos + 2) & 0xff) << 16;
        return length + Packet.PACKET_HEADER_SIZE;
    }

    @Override
    public void handle(NetBuffer buffer) {
        if (lastBuffer != null) {
            buffer = lastBuffer.appendBuffer(buffer);
            lastBuffer = null;
        }

        int length = buffer.length();
        if (length < 4) {
            lastBuffer = buffer;
            return;
        }

        int pos = 0;
        try {
            while (true) {
                // 必须生成新的Transfer实例，不同协议包对应不同Transfer实例，
                // 否则如果有多个CommandHandler线程时会用同一个Transfer实例写数据，这会产生并发问题。
                DataInputStream in;
                if (pos == 0)
                    in = createDataInputStream(buffer);
                else
                    in = createDataInputStream(buffer.slice(pos, pos + length));
                int packetLength = getPacketLength(buffer, pos) - 4;
                if (length - 4 == packetLength) {
                    parsePacket(in, packetLength);
                    break;
                } else if (length - 4 > packetLength) {
                    parsePacket(in, packetLength);
                    pos = pos + packetLength + 4;
                    length = length - (packetLength + 4);
                    // 有可能剩下的不够4个字节了
                    if (length < 4) {
                        lastBuffer = buffer.getBuffer(pos, pos + length);
                        break;
                    } else {
                        continue;
                    }
                } else {
                    lastBuffer = buffer.getBuffer(pos, pos + length);
                    break;
                }
            }
        } catch (Throwable e) {
            logger.error("Parse packet", e);
        }
    }

    private static DataInputStream createDataInputStream(NetBuffer buffer) {
        return new DataInputStream(new NetBufferInputStream(buffer));
    }

    private void parsePacket(DataInputStream in, int packetLength) throws IOException {
        packetLength += 4;
        byte[] packet = new byte[packetLength];
        in.read(packet, 0, packetLength);
        PacketInputImpl input = new PacketInputImpl(packet);
        packetHandler.handle(input);
    }
}
