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
package org.lealone.test.perf.storage;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueString;
import org.lealone.storage.DefaultPageOperationHandler;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.BTreePage;
import org.lealone.storage.aose.btree.PageOperations.Put;
import org.lealone.storage.aose.btree.PageReference;

// -Xms512M -Xmx512M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
public class AsyncBTreePerfTest extends StorageMapPerfTest {

    public static void main(String[] args) throws Exception {
        new AsyncBTreePerfTest().run();
    }

    private final AtomicInteger index = new AtomicInteger(0);
    private BTreeMap<Integer, String> btreeMap;
    private DefaultPageOperationHandler[] handlers;

    @Override
    protected void resetFields() {
        super.resetFields();
        index.set(0);
    }

    @Override
    protected void testWrite(int loop) {
        multiThreadsRandomWriteAsync(loop);
        multiThreadsSerialWriteAsync(loop);
    }

    @Override
    protected void testRead(int loop) {
        multiThreadsRandomRead(loop);
        multiThreadsSerialRead(loop);

        // multiThreadsRandomReadAsync(loop);
        // multiThreadsSerialReadAsync(loop);
    }

    @Override
    protected void testConflict(int loop) {
        testConflict(loop, true);
    }

    @Override
    protected void beforeRun() {
        createPageOperationHandlers();
        super.beforeRun();
        // printLeafPageOperationHandlerPercent();
        // printShiftCount(conflictKeys);
    }

    void printShiftCount(int[] keys) {
        HashMap<PageOperationHandler, Integer> map = new HashMap<>();
        for (int key : keys) {
            BTreePage p = btreeMap.gotoLeafPage(key);
            PageOperationHandler handler = p.getHandler();
            Integer count = map.get(handler);
            if (count == null)
                count = 1;
            else
                count++;
            map.put(handler, count);
        }

        System.out.println("key count: " + keys.length);
        for (HashMap.Entry<PageOperationHandler, Integer> e : map.entrySet()) {
            String percent = String.format("%#.2f", (e.getValue() * 1.0 / keys.length * 100));
            System.out.println(e.getKey() + " percent: " + percent + "%");
        }
        System.out.println();
    }

    void printLeafPageOperationHandlerPercent() {
        BTreePage root = btreeMap.getRootPage();
        HashMap<PageOperationHandler, Integer> map = new HashMap<>();
        AtomicLong leafPageCount = new AtomicLong(0);
        if (root.isLeaf()) {
            map.put(root.getHandler(), 1);
            leafPageCount.incrementAndGet();
        } else {
            findLeafPage(root, map, leafPageCount);
        }
        System.out.println("leaf page count: " + leafPageCount.get());
        System.out.println("handler factory: " + storage.getPageOperationHandlerFactory().getClass().getSimpleName());
        for (HashMap.Entry<PageOperationHandler, Integer> e : map.entrySet()) {
            String percent = String.format("%#.2f", (e.getValue() * 1.0 / leafPageCount.get() * 100));
            System.out.println(e.getKey() + " percent: " + percent + "%");
        }
        System.out.println();
    }

    private void findLeafPage(BTreePage p, HashMap<PageOperationHandler, Integer> map, AtomicLong leafPageCount) {
        if (p.isNode()) {
            for (PageReference ref : p.getChildren()) {
                BTreePage child = ref.getPage();
                if (child.isLeaf()) {
                    PageOperationHandler handler = child.getHandler();
                    // System.out.println("handler: " + handler);
                    Integer count = map.get(handler);
                    if (count == null)
                        count = 1;
                    else
                        count++;
                    map.put(handler, count);
                    leafPageCount.incrementAndGet();
                } else {
                    findLeafPage(child, map, leafPageCount);
                }
            }
        }
    }

    @Override
    protected void init() {
        if (!inited.compareAndSet(false, true))
            return;
        initConfig();
        createPageOperationHandlers();
        openStorage(false);
        openMap();
    }

    private void createPageOperationHandlers() {
        handlers = new DefaultPageOperationHandler[threadCount];
        for (int i = 0; i < threadCount; i++) {
            handlers[i] = new DefaultPageOperationHandler(i, config);
        }
        PageOperationHandlerFactory f = PageOperationHandlerFactory.create(config, handlers);
        f.stopHandlers();
        f.setPageOperationHandlers(handlers);
        f.startHandlers();
    }

    @Override
    protected void openMap() {
        if (map == null || map.isClosed()) {
            map = btreeMap = storage.openBTreeMap(AsyncBTreePerfTest.class.getSimpleName(), ValueInt.type,
                    ValueString.type, null);
        }
    }

    @Override
    protected void printRunResult(int loop, long totalTime, long avgTime, String str) {
        String shiftStr = getShiftStr();

        if (testConflictOnly)
            printResult(loop,
                    ", row count: " + rowCount + ", thread count: " + threadCount + ", conflict keys: "
                            + conflictKeyCount + shiftStr + ", async write conflict, total time: " + totalTime
                            + " ms, avg time: " + avgTime + " ms");
        else
            printResult(loop, ", row count: " + rowCount + ", thread count: " + threadCount + shiftStr + ", async" + str
                    + ", total time: " + totalTime + " ms, avg time: " + avgTime + " ms");
    }

    // 异步场景下线程移交PageOperation的次数
    private String getShiftStr() {
        String shiftStr = "";
        long shiftSum = 0;
        for (int i = 0; i < threadCount; i++) {
            DefaultPageOperationHandler h = handlers[i];
            shiftSum += h.getShiftCount();
        }
        shiftStr = ", shift: " + shiftSum;
        return shiftStr;
    }

    @Override
    protected PerfTestTask createPerfTestTask(int start, int end) throws Exception {
        if (testConflictOnly)
            return new AsyncBTreeConflictPerfTestTask();
        else
            return new AsyncBTreePerfTestTask(start, end);
    }

    class AsyncBTreePerfTestTask extends StorageMapPerfTestTask implements PageOperation {

        PageOperationHandler currentHandler;

        AsyncBTreePerfTestTask(int start, int end) throws Exception {
            super(start, end);
            DefaultPageOperationHandler h = handlers[index.getAndIncrement()];
            h.reset(false);
            h.handlePageOperation(this);
        }

        @Override
        public PageOperationResult run(PageOperationHandler currentHandler) {
            this.currentHandler = currentHandler;
            super.run();
            return PageOperationResult.SUCCEEDED;
        }

        @Override
        public boolean needCreateThread() {
            return false;
        }

        @Override
        protected void read() throws Exception {
            for (int i = start; i < end; i++) {
                int key;
                if (isRandom())
                    key = randomKeys[i];
                else
                    key = i;
                map.get(key, ar -> {
                    notifyOperationComplete();
                });
            }
        }

        @Override
        protected void write() throws Exception {
            for (int i = start; i < end; i++) {
                int key;
                if (isRandom())
                    key = randomKeys[i];
                else
                    key = i;
                String value = "value-";// "value-" + key;

                PageOperation po = new Put<>(btreeMap, key, value, ar -> {
                    notifyOperationComplete();
                });
                po.run(currentHandler);
                // PageOperationResult result = po.run(currentHandler);
                // if (result == PageOperationResult.SHIFTED) {
                // shiftCount++;
                // }
                // currentHandler.handlePageOperation(po);
            }
        }
    }

    class AsyncBTreeConflictPerfTestTask extends AsyncBTreePerfTestTask {

        AsyncBTreeConflictPerfTestTask() throws Exception {
            super(0, conflictKeyCount);
        }

        @Override
        protected void write() throws Exception {
            for (int i = 0; i < conflictKeyCount; i++) {
                int key = conflictKeys[i];
                String value = "value-conflict";

                PageOperation po = new Put<>(btreeMap, key, value, ar -> {
                    notifyOperationComplete();
                });
                po.run(currentHandler);
            }
        }
    }
}
