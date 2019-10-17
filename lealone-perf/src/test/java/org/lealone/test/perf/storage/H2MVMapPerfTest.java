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
package org.lealone.test.perf.storage;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

public class H2MVMapPerfTest extends StorageMapPerfTest {

    public static void main(String[] args) throws Exception {
        new H2MVMapPerfTest().run();
    }

    MVMap<Integer, String> map;

    @Override
    protected void init() {
        if (!inited.compareAndSet(false, true))
            return;
        MVStore store = MVStore.open(null);
        map = store.openMap(H2MVMapPerfTest.class.getSimpleName());
    }

    @Override
    protected void beforeRun() {
        map.clear();
        singleThreadSerialWrite();
    }

    @Override
    protected int size() {
        return map.size();
    }

    @Override
    protected void put(Integer key, String value) {
        map.put(key, value);
    }

    @Override
    protected String get(Integer key) {
        return map.get(key);
    }
}
