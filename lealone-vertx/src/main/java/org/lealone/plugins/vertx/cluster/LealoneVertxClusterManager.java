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
package org.lealone.plugins.vertx.cluster;

import java.util.List;
import java.util.Map;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;

public class LealoneVertxClusterManager implements ClusterManager {

    @SuppressWarnings("unused")
    private Vertx vertx;

    @Override
    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> resultHandler) {
        VertxAsyncMultiMap<K, V> map = new VertxAsyncMultiMap<>();
        resultHandler.handle(Future.succeededFuture(map));
    }

    @Override
    public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> resultHandler) {
        VertxAsyncMap<K, V> map = new VertxAsyncMap<>();
        resultHandler.handle(Future.succeededFuture(map));
    }

    @Override
    public <K, V> Map<K, V> getSyncMap(String name) {
        return new VertxSyncMap<K, V>();
    }

    @Override
    public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
        VertxLock lock = new VertxLock();
        resultHandler.handle(Future.succeededFuture(lock));
    }

    @Override
    public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
        VertxCounter counter = new VertxCounter();
        resultHandler.handle(Future.succeededFuture(counter));
    }

    @Override
    public String getNodeID() {
        return null;
    }

    @Override
    public List<String> getNodes() {
        return null;
    }

    @Override
    public void nodeListener(NodeListener listener) {
    }

    @Override
    public void join(Handler<AsyncResult<Void>> resultHandler) {
    }

    @Override
    public void leave(Handler<AsyncResult<Void>> resultHandler) {
    }

    @Override
    public boolean isActive() {
        return false;
    }
}
