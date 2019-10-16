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
package org.lealone.plugins.test.perf.transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.db.TransactionStore;
import org.h2.mvstore.db.TransactionStore.Transaction;
import org.h2.mvstore.db.TransactionStore.TransactionMap;

public class H2TransactionPerfTest {

    public static void main(String[] args) throws Exception {
        new H2TransactionPerfTest().run();
    }

    static int threadsCount = 4; // Runtime.getRuntime().availableProcessors() * 4;
    static int count = 90000 * 1;// 50000;

    int loop = 20;
    int[] randomKeys = getRandomKeys();
    MVMap<Integer, String> map;
    String mapName = H2TransactionPerfTest.class.getSimpleName();
    TransactionStore ts;
    final AtomicLong startTime = new AtomicLong(0);
    final AtomicLong endTime = new AtomicLong(0);

    public void run() {
        // MVStore.Builder builder = new MVStore.Builder();
        MVStore store = MVStore.open(null);
        map = store.openMap(mapName);
        ts = new TransactionStore(store);
        ts.init();

        singleThreadSerialWrite();
        for (int i = 1; i <= loop; i++) {
            // map.clear();

            // singleThreadRandomWrite();
            // singleThreadSerialWrite();

            // singleThreadRandomRead();
            // singleThreadSerialRead();

            multiThreadsRandomWrite(i);
            multiThreadsSerialWrite(i);

            // multiThreadsRandomRead(i);
            // multiThreadsSerialRead(i);
            System.out.println();
        }
        // System.out.println("map size: " + map.size());
    }

    int[] getRandomKeys() {
        ArrayList<Integer> list = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            list.add(i);
        }
        Collections.shuffle(list);
        int[] keys = new int[count];
        for (int i = 0; i < count; i++) {
            keys[i] = list.get(i);
        }
        return keys;
    }

    void singleThreadSerialWrite() {
        Transaction tx1 = ts.begin();
        TransactionMap<Integer, String> map1 = tx1.openMap(mapName);
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map1.put(i, "valueaaa");
        }
        tx1.commit();
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread serial write time: " + (t2 - t1) + " ms, count: " + count);
    }

    void singleThreadRandomWrite() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.put(randomKeys[i], "valueaaa");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread random write time: " + (t2 - t1) + " ms, count: " + count);
    }

    void singleThreadSerialRead() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.get(i);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread serial read time: " + (t2 - t1) + " ms, count: " + count);
    }

    void singleThreadRandomRead() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.get(randomKeys[i]);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread random read time: " + (t2 - t1) + " ms, count: " + count);
    }

    class MyThread extends Thread {
        int start;
        int end;
        boolean read;
        boolean random;

        MyThread(int start, int end, boolean read, boolean random) {
            super("MyThread-" + start);
            this.start = start;
            this.end = end;
            this.read = read;
            this.random = random;
        }

        void write() throws Exception {
            // 取最早启动的那个线程的时间
            startTime.compareAndSet(0, System.currentTimeMillis());
            Transaction tx1 = ts.begin();
            TransactionMap<Integer, String> map1 = tx1.openMap(mapName);
            for (int i = start; i < end; i++) {
                Integer key;
                if (random)
                    key = randomKeys[i];
                else
                    key = i;
                String value = "value-";// "value-" + key;
                // map.put(key, value);

                Transaction t = ts.begin();
                TransactionMap<Integer, String> m = map1.getInstance(t, 0);
                m.put(key, value);
                t.commit();
            }
        }

        void read() throws Exception {
            for (int i = start; i < end; i++) {
                if (random)
                    map.get(randomKeys[i]);
                else
                    map.get(i);
            }
        }

        @Override
        public void run() {
            try {
                if (read) {
                    read();
                } else {
                    write();
                }
                endTime.set(System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    void multiThreadsSerialRead(int loop) {
        multiThreads(loop, true, false);
    }

    void multiThreadsRandomRead(int loop) {
        multiThreads(loop, true, true);
    }

    void multiThreadsSerialWrite(int loop) {
        multiThreads(loop, false, false);
    }

    void multiThreadsRandomWrite(int loop) {
        multiThreads(loop, false, true);
    }

    void multiThreads(int loop, boolean read, boolean random) {
        startTime.set(0);
        endTime.set(0);
        int avg = count / threadsCount;
        MyThread[] threads = new MyThread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            int start = i * avg;
            int end = (i + 1) * avg;
            if (i == threadsCount - 1)
                end = count;
            threads[i] = new MyThread(start, end, read, random);
        }

        for (int i = 0; i < threadsCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadsCount; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long totalTime = endTime.get() - startTime.get();
        long avgTime = totalTime / threadsCount;
        System.out.println(H2TransactionPerfTest.class.getSimpleName() + " loop: " + loop + ", rows: " + count
                + ", threads: " + threadsCount + (random ? ", random " : ", serial ") + (read ? "read " : "write")
                + ", total time: " + totalTime + " ms, avg time: " + avgTime + " ms");
    }
}
