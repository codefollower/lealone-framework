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
package org.lealone.plugins.test.vertx;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.plugins.vertx.jdbc.LealoneDataSourceProvider;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;

public class VertxJdbcClientTest {

    private static final Logger log = LoggerFactory.getLogger(VertxJdbcClientTest.class);

    static JDBCClient client;

    public static void main(String[] args) {
        JsonObject config = new JsonObject();
        config.put("user", "root");
        config.put("password", "");
        config.put("url", "jdbc:lealone:embed:test");
        config.put("url", "jdbc:lealone:tcp://localhost/lealone");
        config.put("provider_class", LealoneDataSourceProvider.class.getName());
        VertxOptions opt = new VertxOptions();
        opt.setBlockedThreadCheckInterval(Integer.MAX_VALUE);
        Vertx vertx = Vertx.vertx(opt);
        try {
            client = JDBCClient.createShared(vertx, config);

            update("create table t(f1 int, f2 VARCHAR)", res1 -> {
                update("insert into t values(1, 'aa'),(2, 'bb')", res2 -> {
                    query("select * from t", res3 -> {
                        for (JsonArray ja : res3.getResults()) {
                            log.info(ja);
                        }
                        JsonArray params = new JsonArray();
                        params.add(2);
                        query("select * from t where f1=?", params, res4 -> {
                            for (JsonArray ja : res4.getResults()) {
                                log.info(ja);
                            }
                            vertx.close();
                        });
                    });
                });
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void query(String sql, Handler<ResultSet> resultHandler) {
        log.info(sql);
        getConnection(connection -> {
            connection.query(sql, res -> {
                try {
                    if (res.succeeded()) {
                        ResultSet rs = res.result();
                        resultHandler.handle(rs);
                    } else {
                        fail(res.cause());
                    }
                } finally {
                    connection.close();
                }
            });
        });
    }

    static void query(String sql, JsonArray params, Handler<ResultSet> resultHandler) {
        log.info(sql);
        getConnection(connection -> {
            connection.queryWithParams(sql, params, res -> {
                try {
                    if (res.succeeded()) {
                        ResultSet rs = res.result();
                        resultHandler.handle(rs);
                    } else {
                        fail(res.cause());
                    }
                } finally {
                    connection.close();
                }
            });
        });
    }

    static void update(String sql, Handler<UpdateResult> resultHandler) {
        log.info(sql);
        getConnection(connection -> {
            connection.update(sql, res -> {
                try {
                    if (res.succeeded()) {
                        UpdateResult us = res.result();
                        resultHandler.handle(us);
                    } else {
                        fail(res.cause());
                    }
                } finally {
                    connection.close();
                }
            });
        });
    }

    static void getConnection(Handler<SQLConnection> connectionHandler) {
        client.getConnection(res -> {
            if (res.succeeded()) {
                SQLConnection connection = res.result();
                connectionHandler.handle(connection);
            } else {
                fail(res.cause());
            }
        });
    }

    static void fail(Throwable t) {
        log.error(t.getMessage(), t);
    }
}
