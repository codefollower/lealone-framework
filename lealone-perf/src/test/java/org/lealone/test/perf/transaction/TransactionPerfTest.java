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
package org.lealone.test.perf.transaction;

import java.nio.ByteBuffer;

import org.lealone.test.perf.PerfTestBase;

public abstract class TransactionPerfTest extends PerfTestBase {

    public static void run(TransactionPerfTest test) throws Exception {
        test.isRandom = true;
        test.run();
        println();
        test.isRandom = false;
        test.run();
    }

    protected final String mapName = getClass().getSimpleName();

    void testByteBufferAllocate() {
        for (int j = 0; j < 20; j++) {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < 25000; i++) {
                ByteBuffer.allocate(1024 * 1024);
                // DataBuffer writeBuffer = DataBuffer.create(1024 * 1024);
                // try (DataBuffer writeBuffer = DataBuffer.create()) {
                // ByteBuffer buffer = writeBuffer.getAndFlipBuffer();
                // ByteBuffer operations = ByteBuffer.allocateDirect(buffer.limit());
                // operations.put(buffer);
                // operations.flip();
                // }
            }
            long t2 = System.currentTimeMillis();
            printResult("ByteBufferAllocate time: " + (t2 - t1) + " ms");
        }
    }

    protected abstract void write(int start, int end) throws Exception;

    @Override
    protected PerfTestTask createPerfTestTask(int start, int end) throws Exception {
        return new TransactionPerfTestTask(start, end);
    }

    protected class TransactionPerfTestTask extends PerfTestTask {

        TransactionPerfTestTask(int start, int end) throws Exception {
            super(start, end);
        }

        @Override
        public void startPerfTest() throws Exception {
            write(start, end);
        }
    }
}
