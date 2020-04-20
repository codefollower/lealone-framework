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
import java.util.Set;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.shareddata.AsyncMap;

public class VertxAsyncMap<K, V> implements AsyncMap<K, V> {

    @Override
    public void get(K k, Handler<AsyncResult<V>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void remove(K k, Handler<AsyncResult<V>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void replace(K k, V v, Handler<AsyncResult<V>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void clear(Handler<AsyncResult<Void>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void size(Handler<AsyncResult<Integer>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void keys(Handler<AsyncResult<Set<K>>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void values(Handler<AsyncResult<List<V>>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void entries(Handler<AsyncResult<Map<K, V>>> resultHandler) {
        // TODO Auto-generated method stub

    }

}
