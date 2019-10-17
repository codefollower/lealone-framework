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
package org.lealone.test.perf.sql.embed;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.db.SysProperties;
import org.lealone.server.Scheduler;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.test.amte.AMTransactionEngineTest;
import org.lealone.test.perf.sql.SqlPerfTest;
import org.lealone.transaction.TransactionEngine;

public class LealoneEmbeddedSqlPerfTest extends SqlPerfTest {

    public static void main(String[] args) throws Exception {
        new LealoneEmbeddedSqlPerfTest().run();
    }

    private final HashMap<String, String> config = new HashMap<>();
    private final AtomicInteger index = new AtomicInteger(0);
    private Scheduler[] handlers;
    private TransactionEngine te;

    @Override
    protected void resetFields() {
        super.resetFields();
        index.set(0);
    }

    @Override
    protected void init() throws Exception {
        if (!inited.compareAndSet(false, true))
            return;
        SysProperties.setBaseDir(PERF_TEST_BASE_DIR);

        String factoryType = "RoundRobin";
        factoryType = "Random";
        // factoryType = "LoadBalance";
        config.put("page_operation_handler_factory_type", factoryType);
        // config.put("page_operation_handler_count", (threadCount + 1) + "");
        createPageOperationHandlers();

        initTransactionEngineConfig(config);
        te = AMTransactionEngineTest.getTransactionEngine(config);

        super.init();
    }

    @Override
    protected void destroy() throws Exception {
        te.close();
    }

    private void createPageOperationHandlers() {
        handlers = new Scheduler[threadCount];
        HashMap<String, String> config = new HashMap<>();
        for (int i = 0; i < threadCount; i++) {
            handlers[i] = new Scheduler(i, config);
            handlers[i].start();
        }
        PageOperationHandlerFactory.create(null, handlers);
    }

    @Override
    protected Connection getConnection() throws Exception {
        return getLealoneConnection(true);
    }

    @Override
    protected void update(Statement stmt, int start, int end) throws Exception {
        JdbcStatement statement = (JdbcStatement) stmt;
        for (int i = start; i < end; i++) {
            Integer key;
            if (isRandom())
                key = randomKeys[i];
            else
                key = i;
            // String value = "value-";// "value-" + key;
            // map.put(key, value);

            String sql = "update SqlPerfTest set f2 = 'value2' where f1 =" + key;
            statement.executeUpdate(sql);
            notifyOperationComplete();
            // System.out.println(getName() + " key:" + key);
            // AsyncHandler<AsyncResult<Integer>> handler = ar -> {
            // // if (count.decrementAndGet() <= 0) {
            // //
            // // endTime.set(System.currentTimeMillis());
            // // latch.countDown();
            // // }
            // notifyOperationComplete();
            // };
            // statement.executeUpdateAsync(sql, handler);
        }
    }

    @Override
    protected LealoneEmbeddedSqlPerfTestTask createPerfTestTask(int start, int end) throws Exception {
        return new LealoneEmbeddedSqlPerfTestTask(start, end);
    }

    class LealoneEmbeddedSqlPerfTestTask extends SqlPerfTestTask implements PageOperation {

        LealoneEmbeddedSqlPerfTestTask(int start, int end) throws Exception {
            super(start, end);
            Scheduler h = handlers[index.getAndIncrement()];
            h.handlePageOperation(this);
        }

        @Override
        public boolean needCreateThread() {
            return false;
        }
    }
}
