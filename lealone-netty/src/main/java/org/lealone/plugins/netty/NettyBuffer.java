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
package org.lealone.plugins.netty;

import org.lealone.net.NetBuffer;

import io.netty.buffer.ByteBuf;

public class NettyBuffer implements NetBuffer {

    private final ByteBuf buffer;
    private int length;
    private boolean onlyOnePacket;
    private boolean forWrite;

    public NettyBuffer(ByteBuf buffer) {
        this.buffer = buffer;
        this.forWrite = true;
    }

    public NettyBuffer(ByteBuf buffer, boolean onlyOnePacket) {
        this.buffer = buffer;
        this.onlyOnePacket = onlyOnePacket;
        length = buffer.readableBytes();
    }

    public ByteBuf getBuffer() {
        return buffer;
    }

    @Override
    public int length() {
        if (forWrite)
            return buffer.writerIndex();
        return length;
    }

    @Override
    public short getUnsignedByte(int pos) {
        return buffer.getUnsignedByte(pos);
    }

    @Override
    public void read(byte[] dst, int off, int len) {
        buffer.getBytes(off, dst, 0, len);
    }

    @Override
    public NettyBuffer appendByte(byte b) {
        buffer.writeByte(b);
        return this;
    }

    @Override
    public NettyBuffer appendBytes(byte[] bytes, int offset, int len) {
        buffer.writeBytes(bytes, offset, len);
        return this;
    }

    @Override
    public NettyBuffer appendInt(int i) {
        buffer.writeInt(i);
        return this;
    }

    @Override
    public NettyBuffer setByte(int pos, byte b) {
        ensureWritable(pos, 1);
        buffer.setByte(pos, b);
        return this;
    }

    private void ensureWritable(int pos, int len) {
        int ni = pos + len;
        int cap = buffer.capacity();
        int over = ni - cap;
        if (over > 0) {
            buffer.writerIndex(cap);
            buffer.ensureWritable(over);
        }
        // We have to make sure that the writer index is always positioned on the last bit of data set in the buffer
        if (ni > buffer.writerIndex()) {
            buffer.writerIndex(ni);
        }
    }

    @Override
    public NetBuffer flip() {
        return this;
    }

    @Override
    public boolean isOnlyOnePacket() {
        return onlyOnePacket;
    }

    @Override
    public void recycle() {
        // if (forWrite)
        buffer.release();
    }
}
