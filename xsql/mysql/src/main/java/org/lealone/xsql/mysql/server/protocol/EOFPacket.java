/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.xsql.mysql.server.protocol;

import java.nio.ByteBuffer;

import org.lealone.xsql.mysql.server.util.BufferUtil;

/**
 * From Server To Client, at the end of a series of Field Packets, and at the
 * end of a series of Data Packets.With prepared statements, EOF Packet can also
 * end parameter information, which we'll describe later.
 * 
 * <pre>
 * Bytes                 Name
 * -----                 ----
 * 1                     field_count, always = 0xfe
 * 2                     warning_count
 * 2                     Status Flags
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#EOF_Packet
 * </pre>
 * 
 * @author xianmao.hexm 2010-7-16 上午10:55:53
 * @author zhh
 */
public class EOFPacket extends ResponsePacket {

    public byte fieldCount = (byte) 0xfe;
    public int warningCount;
    public int status = 2;

    @Override
    public String getPacketInfo() {
        return "MySQL EOF Packet";
    }

    @Override
    public int calcPacketSize() {
        return 5; // 1+2+2;
    }

    @Override
    public void writeBody(ByteBuffer buffer, PacketOutput out) {
        buffer.put(fieldCount);
        BufferUtil.writeUB2(buffer, warningCount);
        BufferUtil.writeUB2(buffer, status);
    }
}
