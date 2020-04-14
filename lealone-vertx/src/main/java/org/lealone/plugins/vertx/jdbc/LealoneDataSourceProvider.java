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
package org.lealone.plugins.vertx.jdbc;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.lealone.client.jdbc.JdbcDataSource;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.DataSourceProvider;

public class LealoneDataSourceProvider implements DataSourceProvider {

    @Override
    public int maximumPoolSize(DataSource dataSource, JsonObject config) throws SQLException {
        // 不需要maximumPoolSize这个参数
        return -1;
    }

    @Override
    public DataSource getDataSource(JsonObject config) throws SQLException {
        JdbcDataSource ds = new JdbcDataSource();
        String url = config.getString("url");
        if (url == null)
            throw new NullPointerException("url cannot be null");
        String user = config.getString("user");
        String password = config.getString("password");
        String description = config.getString("description");
        ds.setURL(url);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setDescription(description);
        return ds;
    }

    @Override
    public void close(DataSource dataSource) throws SQLException {
        // 什么都不用做
    }
}
