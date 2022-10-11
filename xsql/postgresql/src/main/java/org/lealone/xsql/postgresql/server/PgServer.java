/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.xsql.postgresql.server;

import java.util.HashSet;
import java.util.Map;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.WritableChannel;
import org.lealone.server.AsyncServer;
import org.lealone.server.Scheduler;

public class PgServer extends AsyncServer<PgServerConnection> {

    private static final Logger logger = LoggerFactory.getLogger(PgServer.class);

    public static final String PG_VERSION = "8.2.23";
    public static final String PG_CATALOG_FILE = "/org/lealone/xsql/postgresql/resources/pg_catalog.sql";
    public static final int DEFAULT_PORT = 9510;

    private final HashSet<Integer> typeSet = new HashSet<>();
    private boolean trace;

    @Override
    public String getType() {
        return PgServerEngine.NAME;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        trace = Boolean.parseBoolean(config.get("trace"));
    }

    @Override
    protected int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected PgServerConnection createConnection(WritableChannel writableChannel, Scheduler scheduler) {
        return new PgServerConnection(this, writableChannel, scheduler);
    }

    @Override
    protected void beforeRegister(PgServerConnection conn, Scheduler scheduler) {
        conn.setProcessId(getConnectionSize());
    }

    void trace(String msg) {
        if (trace)
            logger.info(msg);
    }

    void traceError(Throwable e) {
        logger.error("", e);
    }

    boolean getTrace() {
        return trace;
    }

    /**
     * Get the type hash set.
     *
     * @return the type set
     */
    HashSet<Integer> getTypeSet() {
        return typeSet;
    }

    /**
     * Check whether a data type is supported.
     * A warning is logged if not.
     *
     * @param type the type
     */
    void checkType(int type) {
        if (!typeSet.contains(type)) {
            logger.info("Unsupported type: " + type);
        }
    }
}
