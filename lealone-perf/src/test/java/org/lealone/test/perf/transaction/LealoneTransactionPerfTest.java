/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.perf.transaction;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.storage.DefaultPageOperationHandler;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.AOStorageBuilder;
import org.lealone.test.amte.AMTransactionEngineTest;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;

public class LealoneTransactionPerfTest extends TransactionPerfTest {

    public static void main(String[] args) throws Exception {
        LealoneTransactionPerfTest test = new LealoneTransactionPerfTest();
        run(test);
    }

    protected AOStorage storage;
    protected String storagePath;

    private final HashMap<String, String> config = new HashMap<>();
    private final AtomicInteger index = new AtomicInteger(0);
    private DefaultPageOperationHandler[] handlers;
    private TransactionEngine te;

    @Override
    protected void resetFields() {
        super.resetFields();
        index.set(0);
    }

    @Override
    protected void init() throws Exception {
        String factoryType = "RoundRobin";
        factoryType = "Random";
        // factoryType = "LoadBalance";
        config.put("page_operation_handler_factory_type", factoryType);
        // config.put("page_operation_handler_count", (threadCount + 1) + "");
        createPageOperationHandlers();

        AOStorageBuilder builder = new AOStorageBuilder(config);
        storagePath = joinDirs("lealone", "aose");
        int pageSplitSize = 16 * 1024;
        builder.storagePath(storagePath).compress().reuseSpace().pageSplitSize(pageSplitSize).minFillRate(30);
        storage = builder.openStorage();

        initTransactionEngineConfig(config);
        te = AMTransactionEngineTest.getTransactionEngine(config);

        singleThreadSerialWrite();
    }

    @Override
    protected void destroy() throws Exception {
        te.close();
        storage.close();
    }

    private void createPageOperationHandlers() {
        handlers = new DefaultPageOperationHandler[threadCount];
        for (int i = 0; i < threadCount; i++) {
            handlers[i] = new DefaultPageOperationHandler(i, config);
        }
        PageOperationHandlerFactory f = PageOperationHandlerFactory.create(config, handlers);
        f.startHandlers();
    }

    private void singleThreadSerialWrite() {
        Transaction t = te.beginTransaction(false);
        TransactionMap<Integer, String> map = t.openMap(mapName, storage);
        map.clear();
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            map.put(i, "valueaaa");
        }
        t.commit();
        long t2 = System.currentTimeMillis();

        printResult("single-thread serial write time: " + (t2 - t1) + " ms, row count: " + map.size());
    }

    @Override
    protected void write(int start, int end) throws Exception {
        Transaction t = te.beginTransaction(false);
        TransactionMap<Integer, String> map = t.openMap(mapName, storage);
        for (int i = start; i < end; i++) {
            Integer key;
            if (isRandom())
                key = randomKeys[i];
            else
                key = i;
            String value = "value-";// "value-" + key;
            // map.put(key, value);

            Transaction t2 = te.beginTransaction(false);
            TransactionMap<Integer, String> m = map.getInstance(t2);
            m.tryUpdate(key, value);
            t2.commit();
            // System.out.println(getName() + " key:" + key);
            notifyOperationComplete();
        }
    }

    @Override
    protected PerfTestTask createPerfTestTask(int start, int end) throws Exception {
        return new LealoneTransactionPerfTestTask(start, end);
    }

    class LealoneTransactionPerfTestTask extends TransactionPerfTestTask implements PageOperation {

        LealoneTransactionPerfTestTask(int start, int end) throws Exception {
            super(start, end);
            DefaultPageOperationHandler h = handlers[index.getAndIncrement()];
            h.reset(false);
            h.handlePageOperation(this);
        }

        @Override
        public boolean needCreateThread() {
            return false;
        }
    }
}
