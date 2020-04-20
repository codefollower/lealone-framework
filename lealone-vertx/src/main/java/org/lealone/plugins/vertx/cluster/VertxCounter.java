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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.shareddata.Counter;

public class VertxCounter implements Counter {

    @Override
    public void get(Handler<AsyncResult<Long>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {
        // TODO Auto-generated method stub

    }
}
