/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.embed;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.RunMode;

public class SqlTestBase extends TestBase {

    protected Connection conn;
    protected Statement stmt;
    protected ResultSet rs;
    protected String sql;
    protected RunMode runMode;

    protected SqlTestBase() {
        // addConnectionParameter("TRACE_LEVEL_FILE", TraceSystem.ADAPTER + "");
    }

    protected SqlTestBase(String dbName) {
        this.dbName = dbName;
    }

    protected SqlTestBase(String dbName, RunMode runMode) {
        this.dbName = dbName;
        this.runMode = runMode;
    }

    protected SqlTestBase(String user, String password) {
        this.user = user;
        this.password = password;
    }

    protected Throwable getRootCause(Throwable cause) {
        return DbException.getRootCause(cause);
    }

    protected boolean autoStartTcpServer() {
        return false;
    }

    protected void test() throws Exception {
        // do nothing
    }

    public int executeUpdate(String sql) {
        try {
            return stmt.executeUpdate(sql);
        } catch (SQLException e) {
            // e.printStackTrace();
            // return -1;
            throw new RuntimeException(e);
        }
    }

    public int executeUpdate() {
        return executeUpdate(sql);
    }

    public void tryExecuteUpdate() {
        tryExecuteUpdate(sql);
    }

    public void tryExecuteUpdate(String sql) {
        try {
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void createTable(String tableName) {
        executeUpdate("DROP TABLE IF EXISTS " + tableName);
        executeUpdate("CREATE TABLE " + tableName + " (pk varchar(100) NOT NULL PRIMARY KEY, " + //
                "f1 varchar(100), f2 varchar(100), f3 int)");
    }

    private void check() throws Exception {
        if (rs == null)
            executeQuery();
    }

    public int getIntValue(int i) throws Exception {
        check();
        return rs.getInt(i);
    }

    public int getIntValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getInt(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public long getLongValue(int i) throws Exception {
        check();
        return rs.getLong(i);
    }

    public long getLongValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getLong(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public double getDoubleValue(int i) throws Exception {
        check();
        return rs.getDouble(i);
    }

    public double getDoubleValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getDouble(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public String getStringValue(int i) throws Exception {
        check();
        return rs.getString(i);
    }

    public String getStringValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getString(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public boolean getBooleanValue(int i) throws Exception {
        check();
        return rs.getBoolean(i);
    }

    public boolean getBooleanValue(int i, boolean closeResultSet) throws Exception {
        check();
        try {
            return rs.getBoolean(i);
        } finally {
            if (closeResultSet)
                closeResultSet();
        }
    }

    public void executeQuery() {
        executeQuery(sql);
    }

    public void executeQuery(String sql) {
        try {
            rs = stmt.executeQuery(sql);
            rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void closeResultSet() throws Exception {
        rs.close();
        rs = null;
    }

    public boolean next() throws Exception {
        check();
        return rs.next();
    }

    public int printResultSet() {
        int count = 0;
        try {
            rs = stmt.executeQuery(sql);
            count = printResultSet(rs);
            rs = null;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }
}
