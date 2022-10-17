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
package org.lealone.plugins.vertx.net;

import org.lealone.net.NetBuffer;

import io.vertx.core.buffer.Buffer;

public class VertxBuffer implements NetBuffer {

    private final Buffer buffer;

    public VertxBuffer(Buffer buff) {
        this.buffer = buff;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    @Override
    public int length() {
        return buffer.length();
    }

    @Override
    public short getUnsignedByte(int pos) {
        return buffer.getUnsignedByte(pos);
    }

    @Override
    public void read(byte[] dst, int off, int len) {
        buffer.getBytes(off, off + len, dst);
    }

    @Override
    public VertxBuffer appendByte(byte b) {
        buffer.appendByte(b);
        return this;
    }

    @Override
    public VertxBuffer appendBytes(byte[] bytes, int offset, int len) {
        buffer.appendBytes(bytes, offset, len);
        return this;
    }

    @Override
    public VertxBuffer appendInt(int i) {
        buffer.appendInt(i);
        return this;
    }

    @Override
    public VertxBuffer setByte(int pos, byte b) {
        buffer.setByte(pos, b);
        return this;
    }

    @Override
    public NetBuffer flip() {
        return this;
    }
}
