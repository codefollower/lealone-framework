/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import org.lealone.bench.BenchTest;
import org.lealone.bench.DbType;
import org.lealone.db.ConnectionSetting;
import org.lealone.db.Constants;
import org.lealone.xsql.postgresql.server.PgServer;

public abstract class ClientServerBTest extends BenchTest {

    private DbType dbType;
    boolean disableLealoneQueryCache = true;

    public void run(DbType dbType) {
        this.dbType = dbType;
        try {
            if (dbType == DbType.Lealone && disableLealoneQueryCache) {
                disableLealoneQueryCache();
            }
            run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected Connection getConnection() throws Exception {
        switch (dbType) {
        case H2:
            return getH2Connection();
        case MySQL:
            return getMySQLConnection();
        case PostgreSQL:
            return getPgConnection();
        case Lealone:
            return getLealoneConnection();
        default:
            throw new RuntimeException();
        }
    }

    protected String getBTestName() {
        return getClass().getSimpleName();
    }

    protected static void close(AutoCloseable... acArray) {
        for (AutoCloseable ac : acArray) {
            try {
                ac.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected static void initData(Statement statement) throws Exception {
        statement.executeUpdate("drop table if exists test");
        statement.executeUpdate("create table if not exists test(name varchar, f1 int,f2 int)");

        statement.getConnection().setAutoCommit(false);
        statement.executeUpdate("insert into test values('abc1',1,2)");
        statement.executeUpdate("insert into test values('abc2',2,2)");
        statement.executeUpdate("insert into test values('abc3',3,2)");
        statement.executeUpdate("insert into test values('abc1',1,2)");
        statement.executeUpdate("insert into test values('abc2',2,2)");
        statement.executeUpdate("insert into test values('abc3',3,2)");
        statement.getConnection().commit();
        statement.getConnection().setAutoCommit(true);
    }

    public static Connection getMySQLConnection() throws Exception {
        String db = "test";
        String user = "test";
        String password = "test";
        int port = 3306;
        String url = "jdbc:mysql://localhost:" + port + "/" + db;

        Properties info = new Properties();
        info.put("user", user);
        info.put("password", password);
        // info.put("holdResultsOpenOverStatementClose","true");
        // info.put("allowMultiQueries","true");

        // info.put("useServerPrepStmts", "true");
        // info.put("cachePrepStmts", "true");
        // info.put("rewriteBatchedStatements", "true");
        // info.put("useCompression", "true");
        info.put("serverTimezone", "GMT");

        Connection conn = DriverManager.getConnection(url, info);
        // conn.setAutoCommit(true);
        return conn;
    }

    public static Connection getPgConnection() throws Exception {
        String url = "jdbc:postgresql://localhost:" + 5432 + "/test";
        return getConnection(url, "test", "test");
    }

    public static Connection getH2Connection() throws Exception {
        String url = "jdbc:h2:tcp://localhost:9092/mydb";
        return getConnection(url, "sa", "");
    }

    public static String getLealoneUrl() {
        String url = "jdbc:lealone:tcp://localhost:" + Constants.DEFAULT_TCP_PORT + "/lealone";
        url += "?" + ConnectionSetting.NETWORK_TIMEOUT + "=" + Integer.MAX_VALUE;
        return url;
    }

    public static Connection getLealoneConnection() throws Exception {
        String url = getLealoneUrl();
        url += "&" + ConnectionSetting.IS_SHARED + "=false";
        return getConnection(url, "root", "");
    }

    public static Connection getLealoneSharedConnection(int maxSharedSize) throws Exception {
        String url = getLealoneUrl();
        url += "&" + ConnectionSetting.IS_SHARED + "=true";
        url += "&" + ConnectionSetting.MAX_SHARED_SIZE + "=" + maxSharedSize;
        return getConnection(url, "root", "");
    }

    public static Connection getLealonePgConnection() throws Exception {
        return getPgConnection(PgServer.DEFAULT_PORT);
    }

    public static Connection getPgConnection(int port) throws Exception {
        String url = "jdbc:postgresql://localhost:" + port + "/test";
        return getConnection(url, "test", "test");
    }

    public static void disableLealoneQueryCache() {
        try {
            Connection conn = getLealoneConnection();
            Statement statement = conn.createStatement();
            statement.executeUpdate("set QUERY_CACHE_SIZE 0");
            statement.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
