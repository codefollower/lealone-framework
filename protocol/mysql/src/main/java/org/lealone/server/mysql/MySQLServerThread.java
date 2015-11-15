/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.server.mysql;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.exceptions.JdbcSQLException;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.common.util.SmallMap;
import org.lealone.common.util.StringUtils;
import org.lealone.db.CommandParameter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.result.Result;
import org.lealone.db.value.Transfer;
import org.lealone.db.value.Value;
import org.lealone.server.mysql.protocol.AuthPacket;
import org.lealone.server.mysql.protocol.Command;
import org.lealone.server.mysql.protocol.ErrorPacket;
import org.lealone.server.mysql.protocol.HandshakePacket;
import org.lealone.server.mysql.protocol.OkPacket;
import org.lealone.sql.BatchStatement;
import org.lealone.sql.PreparedStatement;

/**
 * One server thread is opened per client connection.
 * 
 * @author H2 Group
 * @author zhh
 */
public class MySQLServerThread implements Runnable {

    private static final int BUFFER_SIZE = 16 * 1024;

    private final SmallMap cache = new SmallMap(SysProperties.SERVER_CACHED_OBJECTS);
    private final SmallLRUCache<Long, CachedInputStream> lobs = SmallLRUCache.newInstance(Math.max(
            SysProperties.SERVER_CACHED_OBJECTS, SysProperties.SERVER_RESULT_SET_FETCH_SIZE * 5));

    private final MySQLServer server;
    private final int threadId;
    private final Transfer transfer;

    private Session session;
    private boolean stop;
    private Thread thread;
    private String sessionId;

    private final DataInputStream in;
    private final DataOutputStream out;

    protected MySQLServerThread(Socket socket, MySQLServer server, int threadId) throws IOException {
        this.server = server;
        this.threadId = threadId;
        transfer = new Transfer(null, socket);

        in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE));
    }

    private void trace(String s) {
        server.trace(this + " " + s);
    }

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

    private static String getURL(String db, String host, int port) {
        StringBuilder buff = new StringBuilder();
        buff.append(Constants.URL_PREFIX).append(Constants.URL_TCP);
        buff.append("//").append(host).append(':').append(port).append('/').append(db);
        return buff.toString();
    }

    @Override
    public void run() {
        try {
            transfer.init();
            if (server.isTraceEnabled())
                trace("Connect");
            // TODO server: should support a list of allowed databases
            // and a list of allowed clients
            try {
                if (!server.allow(transfer.getSocket())) {
                    throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
                }
                HandshakePacket.create(threadId).write(out);

                AuthPacket ap = new AuthPacket();
                byte[] packet = readPacket(ap);
                ap.read(packet);

                String db = ap.database;
                String originalURL = getURL(db, server.getListenAddress(), server.getPort());
                String userName = ap.user;
                userName = StringUtils.toUpperEnglish(userName);
                session = createSession(db, originalURL, userName, ap.password);
                // server.addConnection(threadId, originalURL, userName);
                if (server.isTraceEnabled())
                    trace("Connected");

                OkPacket ok = new OkPacket();
                ok.packetId = ap.packetId;
                ok.write(out);
            } catch (Throwable e) {
                sendError(e);
                stop = true;
            }
            while (!stop) {
                try {
                    process();
                } catch (Throwable e) {
                    if (server.isTraceEnabled())
                        server.traceError(e);
                    sendError(e);
                }
            }
            if (server.isTraceEnabled())
                trace("Disconnect");
        } catch (Throwable e) {
            server.traceError(e);
        } finally {
            close();
        }
    }

    private Session createSession(String dbName, String originalURL, String userName, byte[] password)
            throws IOException {
        // byte[] userPasswordHash = transfer.readBytes();
        // byte[] filePasswordHash = transfer.readBytes();
        // byte[] fileEncryptionKey = transfer.readBytes();

        if (dbName == null) // TODO
            dbName = "test";

        dbName = server.checkKeyAndGetDatabaseName(dbName);
        ConnectionInfo ci = new ConnectionInfo(originalURL, dbName);
        ci.disableAuthentication();
        ci.setProperty("DEFAULT_SQL_ENGINE", "MySQL");

        // Properties originalProperties = new Properties();
        //
        // String key, value;
        // int len = transfer.readInt();
        // for (int i = 0; i < len; i++) {
        // key = transfer.readString();
        // value = transfer.readString();
        // ci.addProperty(key, value, true); // 一些不严谨的client driver可能会发送重复的属性名
        // originalProperties.setProperty(key, value);
        // }

        String baseDir = server.getBaseDir();
        if (baseDir == null) {
            baseDir = SysProperties.getBaseDirSilently();
        }

        // override client's requested properties with server settings
        if (baseDir != null) {
            ci.setBaseDir(baseDir);
        }
        if (server.getIfExists()) {
            ci.setProperty("IFEXISTS", "TRUE");
        }
        ci.setUserName(userName);
        ci.setUserPasswordHash(password);
        // ci.setFilePasswordHash(filePasswordHash);
        // ci.setFileEncryptionKey(fileEncryptionKey);

        try {
            Session session = ci.getSessionFactory().createSession(ci);
            // session.setOriginalProperties(originalProperties);
            // session.setLocal(ci.getProperty("IS_LOCAL", false));

            String sql = "create global temporary table if not exists variables"
                    + "(variable_name varchar not null primary key, value varchar)";
            session.parseStatement(sql).executeUpdate();
            return session;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    private void closeSession() {
        if (session != null) {
            RuntimeException closeError = null;
            try {
                session.prepareStatement("ROLLBACK", -1).executeUpdate();
            } catch (RuntimeException e) {
                closeError = e;
                server.traceError(e);
            } catch (Exception e) {
                server.traceError(e);
            }
            try {
                session.close();
                server.removeConnection(threadId);
            } catch (RuntimeException e) {
                if (closeError == null) {
                    closeError = e;
                    server.traceError(e);
                }
            } catch (Exception e) {
                server.traceError(e);
            } finally {
                session = null;
            }
            if (closeError != null) {
                throw closeError;
            }
        }
    }

    /**
     * Close a connection.
     */
    void close() {
        try {
            stop = true;
            closeSession();
        } catch (Exception e) {
            server.traceError(e);
        } finally {
            transfer.close();
            if (server.isTraceEnabled())
                trace("Close");
            server.remove(this);
        }
    }

    private void sendError(Throwable t) {
        t.printStackTrace();
        try {
            SQLException e = DbException.convert(t).getSQLException();
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String trace = writer.toString();
            String message;
            String sql;
            if (e instanceof JdbcSQLException) {
                JdbcSQLException j = (JdbcSQLException) e;
                message = j.getOriginalMessage();
                sql = j.getSQL();
            } else {
                message = e.getMessage();
                sql = null;
            }
            ErrorPacket error = new ErrorPacket();
            error.errno = e.getErrorCode();
            error.sqlState = e.getSQLState().getBytes();
            error.message = message.getBytes();
            error.write(out);
        } catch (Exception e2) {
            if (!transfer.isClosed()) {
                server.traceError(e2);
            }
            // if writing the error does not work, close the connection
            stop = true;
        }
    }

    private void setParameters(PreparedStatement command) throws IOException {
        int len = transfer.readInt();
        ArrayList<? extends CommandParameter> params = command.getParameters();
        for (int i = 0; i < len; i++) {
            CommandParameter p = params.get(i);
            p.setValue(transfer.readValue());
        }
    }

    private void executeBatch(int size, BatchStatement command) throws IOException {
        int old = session.getModificationId();
        synchronized (session) {
            command.executeUpdate();
        }

        int status;
        if (session.isClosed()) {
            status = Session.STATUS_CLOSED;
        } else {
            status = getState(old);
        }
        transfer.writeInt(status);
        int[] result = command.getResult();
        command.close();
        for (int i = 0; i < size; i++)
            transfer.writeInt(result[i]);
        transfer.flush();
    }

    private void process() throws IOException {
        byte[] packet = readPacket();
        int operation = packet[0] & 0xff;
        switch (operation) {
        case Command.QUERY: {
            String sql = new String(packet, 1, packet.length - 1);
            System.out.println(sql);
            int old = session.getModificationId();
            PreparedStatement command = session.prepareStatement(sql, -1);
            boolean readonly = command.isReadOnly();
            // cache.addObject(id, command);
            boolean isQuery = command.isQuery();
            if (isQuery) {
                Result result;
                synchronized (session) {
                    result = command.executeQuery(-1, false);
                }
                // cache.addObject(objectId, result);
                int columnCount = result.getVisibleColumnCount();
                int state = getState(old);
                // transfer.writeInt(state);

                // transfer.writeInt(columnCount);
                int rowCount = result.getRowCount();
                // transfer.writeInt(rowCount);
                // for (int i = 0; i < columnCount; i++) {
                // writeColumn(transfer, result, i);
                // }
                // int fetch = fetchSize;
                // if (rowCount != -1)
                // fetch = Math.min(rowCount, fetchSize);
                // sendRow(result, fetch);
                sendRow(result, -1);
                break;
            }
        }
        // case Session.COMMAND_PREPARE_READ_PARAMS:
        // case Session.COMMAND_PREPARE: {
        // int id = transfer.readInt();
        // String sql = transfer.readString();
        // boolean isReplication = transfer.readBoolean();
        // int old = session.getModificationId();
        // PreparedStatement command = session.prepareStatement(sql, -1);
        // command.setReplication(isReplication);
        // boolean readonly = command.isReadOnly();
        // cache.addObject(id, command);
        // boolean isQuery = command.isQuery();
        // ArrayList<? extends CommandParameter> params = command.getParameters();
        // transfer.writeInt(getState(old)).writeBoolean(isQuery).writeBoolean(readonly).writeInt(params.size());
        // if (operation == Session.COMMAND_PREPARE_READ_PARAMS) {
        // for (CommandParameter p : params) {
        // writeMetaData(transfer, p);
        // }
        // }
        // transfer.flush();
        // break;
        // }
        // // case Session.SESSION_CLOSE: {
        // // stop = true;
        // // closeSession();
        // // transfer.writeInt(Session.STATUS_OK).flush();
        // // close();
        // // break;
        // // }
        // case Session.COMMAND_GET_META_DATA: {
        // int id = transfer.readInt();
        // int objectId = transfer.readInt();
        // PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
        // Result result = command.getMetaData();
        // cache.addObject(objectId, result);
        // int columnCount = result.getVisibleColumnCount();
        // transfer.writeInt(Session.STATUS_OK).writeInt(columnCount).writeInt(0);
        // for (int i = 0; i < columnCount; i++) {
        // writeColumn(transfer, result, i);
        // }
        // transfer.flush();
        // break;
        // }
        // case Session.COMMAND_EXECUTE_PREPARED_QUERY: {
        // int id = transfer.readInt();
        // int objectId = transfer.readInt();
        // int maxRows = transfer.readInt();
        // int fetchSize = transfer.readInt();
        // PreparedStatement command = (PreparedStatement) cache.getObject(id, false);
        // command.setFetchSize(fetchSize);
        // setParameters(command);
        // int old = session.getModificationId();
        // Result result;
        // synchronized (session) {
        // result = command.executeQuery(maxRows, false);
        // }
        // cache.addObject(objectId, result);
        // int columnCount = result.getVisibleColumnCount();
        // int state = getState(old);
        // transfer.writeInt(state);
        //
        // if (operation == Session.COMMAND_EXECUTE_DISTRIBUTED_QUERY)
        // transfer.writeString(session.getTransaction().getLocalTransactionNames());
        //
        // transfer.writeInt(columnCount);
        // int rowCount = result.getRowCount();
        // transfer.writeInt(rowCount);
        // for (int i = 0; i < columnCount; i++) {
        // writeColumn(transfer, result, i);
        // }
        // int fetch = fetchSize;
        // if (rowCount != -1)
        // fetch = Math.min(rowCount, fetchSize);
        // sendRow(result, fetch);
        // transfer.flush();
        // break;
        // }
        // case Session.COMMAND_EXECUTE_BATCH_UPDATE_STATEMENT: {
        // int size = transfer.readInt();
        // ArrayList<String> batchCommands = New.arrayList(size);
        // for (int i = 0; i < size; i++)
        // batchCommands.add(transfer.readString());
        //
        // BatchStatement command = session.getBatchStatement(batchCommands);
        // executeBatch(size, command);
        // break;
        // }
        // case Session.COMMAND_EXECUTE_BATCH_UPDATE_PREPAREDSTATEMENT: {
        // int id = transfer.readInt();
        // int size = transfer.readInt();
        // PreparedStatement preparedCommand = (PreparedStatement) cache.getObject(id, false);
        // ArrayList<Value[]> batchParameters = New.arrayList(size);
        // int paramsSize = preparedCommand.getParameters().size();
        // Value[] values;
        // for (int i = 0; i < size; i++) {
        // values = new Value[paramsSize];
        // for (int j = 0; j < paramsSize; j++) {
        // values[j] = transfer.readValue();
        // }
        // batchParameters.add(values);
        // }
        // BatchStatement command = session.getBatchStatement(preparedCommand, batchParameters);
        // executeBatch(size, command);
        // break;
        // }
        // case Session.COMMAND_CLOSE: {
        // int id = transfer.readInt();
        // PreparedStatement command = (PreparedStatement) cache.getObject(id, true);
        // if (command != null) {
        // command.close();
        // cache.freeObject(id);
        // }
        // break;
        // }
        // case Session.RESULT_FETCH_ROWS: {
        // int id = transfer.readInt();
        // int count = transfer.readInt();
        // Result result = (Result) cache.getObject(id, false);
        // transfer.writeInt(Session.STATUS_OK);
        // sendRow(result, count);
        // transfer.flush();
        // break;
        // }
        // case Session.RESULT_RESET: {
        // int id = transfer.readInt();
        // Result result = (Result) cache.getObject(id, false);
        // result.reset();
        // break;
        // }
        // case Session.RESULT_CLOSE: {
        // int id = transfer.readInt();
        // Result result = (Result) cache.getObject(id, true);
        // if (result != null) {
        // result.close();
        // cache.freeObject(id);
        // }
        // break;
        // }
        // case Session.RESULT_CHANGE_ID: {
        // int oldId = transfer.readInt();
        // int newId = transfer.readInt();
        // Object obj = cache.getObject(oldId, false);
        // cache.freeObject(oldId);
        // cache.addObject(newId, obj);
        // break;
        // }
        // case Session.SESSION_SET_ID: {
        // sessionId = transfer.readString();
        // transfer.writeInt(Session.STATUS_OK).flush();
        // transfer.writeBoolean(session.isAutoCommit());
        // transfer.flush();
        // break;
        // }
        // case Session.SESSION_SET_AUTO_COMMIT: {
        // boolean autoCommit = transfer.readBoolean();
        // session.setAutoCommit(autoCommit);
        // transfer.writeInt(Session.STATUS_OK).flush();
        // break;
        // }
        // case Session.LOB_READ: {
        // long lobId = transfer.readLong();
        // byte[] hmac = transfer.readBytes();
        // CachedInputStream in = lobs.get(lobId);
        // if (in == null) {
        // in = new CachedInputStream(null);
        // lobs.put(lobId, in);
        // }
        // long offset = transfer.readLong();
        // int length = transfer.readInt();
        // transfer.verifyLobMac(hmac, lobId);
        // if (in.getPos() != offset) {
        // LobStorage lobStorage = session.getDataHandler().getLobStorage();
        // // only the lob id is used
        // ValueLob lob = ValueLob.create(Value.BLOB, null, -1, lobId, hmac, -1);
        // InputStream lobIn = lobStorage.getInputStream(lob, hmac, -1);
        // in = new CachedInputStream(lobIn);
        // lobs.put(lobId, in);
        // lobIn.skip(offset);
        // }
        // // limit the buffer size
        // length = Math.min(16 * Constants.IO_BUFFER_SIZE, length);
        // byte[] buff = new byte[length];
        // length = IOUtils.readFully(in, buff, length);
        // transfer.writeInt(Session.STATUS_OK);
        // transfer.writeInt(length);
        // transfer.writeBytes(buff, 0, length);
        // transfer.flush();
        // break;
        // }
        default:
            if (server.isTraceEnabled())
                trace("Unknown operation: " + operation);
            closeSession();
            close();
        }
    }

    private int getState(int oldModificationId) {
        if (session.getModificationId() == oldModificationId) {
            return Session.STATUS_OK;
        }
        return Session.STATUS_OK_STATE_CHANGED;
    }

    private void sendRow(Result result, int count) throws IOException {
        try {
            int visibleColumnCount = result.getVisibleColumnCount();
            for (int i = 0; i < count; i++) {
                if (result.next()) {
                    transfer.writeBoolean(true);
                    Value[] v = result.currentRow();
                    for (int j = 0; j < visibleColumnCount; j++) {
                        transfer.writeValue(v[j]);
                    }
                } else {
                    transfer.writeBoolean(false);
                    break;
                }
            }
        } catch (Throwable e) {
            // 如果取结果集的下一行记录时发生了异常，
            // 比如在HBase环境一个结果集可能涉及多个region，当切换到下一个region时此region有可能在进行split，
            // 此时就会抛异常，所以结果集包必须加一个结束标记，结果集包后面跟一个异常包。
            transfer.writeBoolean(false);
            throw DbException.convert(e);
        }
    }

    void setThread(Thread thread) {
        this.thread = thread;
    }

    Thread getThread() {
        return thread;
    }

    /**
     * Cancel a running statement.
     *
     * @param targetSessionId the session id
     * @param statementId the statement to cancel
     */
    void cancelStatement(String targetSessionId, int statementId) {
        if (StringUtils.equals(targetSessionId, this.sessionId)) {
            PreparedStatement cmd = (PreparedStatement) cache.getObject(statementId, false);
            cmd.cancel();
        }
    }

    /**
     * An input stream with a position.
     */
    static class CachedInputStream extends FilterInputStream {

        private static final ByteArrayInputStream DUMMY = new ByteArrayInputStream(new byte[0]);
        private long pos;

        CachedInputStream(InputStream in) {
            super(in == null ? DUMMY : in);
            if (in == null) {
                pos = -1;
            }
        }

        @Override
        public int read(byte[] buff, int off, int len) throws IOException {
            len = super.read(buff, off, len);
            if (len > 0) {
                pos += len;
            }
            return len;
        }

        @Override
        public int read() throws IOException {
            int x = in.read();
            if (x >= 0) {
                pos++;
            }
            return x;
        }

        @Override
        public long skip(long n) throws IOException {
            n = super.skip(n);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        public long getPos() {
            return pos;
        }

    }

    /**
     * Write the parameter meta data to the transfer object.
     *
     * @param transfer the transfer object
     * @param p the parameter
     */
    private static void writeMetaData(Transfer transfer, CommandParameter p) throws IOException {
        transfer.writeInt(p.getType());
        transfer.writeLong(p.getPrecision());
        transfer.writeInt(p.getScale());
        transfer.writeInt(p.getNullable());
    }

    /**
     * Write a result column to the given output.
     *
     * @param out the object to where to write the data
     * @param result the result
     * @param i the column index
     */
    public static void writeColumn(Transfer out, Result result, int i) throws IOException {
        out.writeString(result.getAlias(i));
        out.writeString(result.getSchemaName(i));
        out.writeString(result.getTableName(i));
        out.writeString(result.getColumnName(i));
        out.writeInt(result.getColumnType(i));
        out.writeLong(result.getColumnPrecision(i));
        out.writeInt(result.getColumnScale(i));
        out.writeInt(result.getDisplaySize(i));
        out.writeBoolean(result.isAutoIncrement(i));
        out.writeInt(result.getNullable(i));
    }

}
