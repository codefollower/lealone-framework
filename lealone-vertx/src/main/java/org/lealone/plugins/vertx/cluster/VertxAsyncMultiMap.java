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

import java.util.function.Predicate;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;

public class VertxAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {

    @Override
    public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
        // TODO Auto-generated method stub

    }
}
