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
package org.lealone.plugins.mysql.protocol;

public interface Command {
    int COM_BINLOG_DUMP = 18;

    int COM_CHANGE_USER = 17;

    int COM_CLOSE_STATEMENT = 25;

    int COM_CONNECT_OUT = 20;

    int COM_END = 29;

    int COM_EXECUTE = 23;

    int COM_FETCH = 28;

    int COM_LONG_DATA = 24;

    int COM_PREPARE = 22;

    int COM_REGISTER_SLAVE = 21;

    int COM_RESET_STMT = 26;

    int COM_SET_OPTION = 27;

    int COM_TABLE_DUMP = 19;

    int CONNECT = 11;

    int CREATE_DB = 5;

    int DEBUG = 13;

    int DELAYED_INSERT = 16;

    int DROP_DB = 6;

    int FIELD_LIST = 4;

    int FIELD_TYPE_BIT = 16;

    int FIELD_TYPE_BLOB = 252;

    int FIELD_TYPE_DATE = 10;

    int FIELD_TYPE_DATETIME = 12;

    // Data Types
    int FIELD_TYPE_DECIMAL = 0;

    int FIELD_TYPE_DOUBLE = 5;

    int FIELD_TYPE_ENUM = 247;

    int FIELD_TYPE_FLOAT = 4;

    int FIELD_TYPE_GEOMETRY = 255;

    int FIELD_TYPE_INT24 = 9;

    int FIELD_TYPE_LONG = 3;

    int FIELD_TYPE_LONG_BLOB = 251;

    int FIELD_TYPE_LONGLONG = 8;

    int FIELD_TYPE_MEDIUM_BLOB = 250;

    int FIELD_TYPE_NEW_DECIMAL = 246;

    int FIELD_TYPE_NEWDATE = 14;

    int FIELD_TYPE_NULL = 6;

    int FIELD_TYPE_SET = 248;

    int FIELD_TYPE_SHORT = 2;

    int FIELD_TYPE_STRING = 254;

    int FIELD_TYPE_TIME = 11;

    int FIELD_TYPE_TIMESTAMP = 7;

    int FIELD_TYPE_TINY = 1;

    // Older data types
    int FIELD_TYPE_TINY_BLOB = 249;

    int FIELD_TYPE_VAR_STRING = 253;

    int FIELD_TYPE_VARCHAR = 15;

    // Newer data types
    int FIELD_TYPE_YEAR = 13;

    int INIT_DB = 2;

    long LENGTH_BLOB = 65535;

    long LENGTH_LONGBLOB = 4294967295L;

    long LENGTH_MEDIUMBLOB = 16777215;

    long LENGTH_TINYBLOB = 255;

    // Limitations
    int MAX_ROWS = 50000000; // From the MySQL FAQ

    /**
     * Used to indicate that the server sent no field-level character set
     * information, so the driver should use the connection-level character
     * encoding instead.
     */
    int NO_CHARSET_INFO = -1;

    byte OPEN_CURSOR_FLAG = 1;

    int PING = 14;

    int PROCESS_INFO = 10;

    int PROCESS_KILL = 12;

    int QUERY = 3;

    int QUIT = 1;

    // ~ Methods
    // ----------------------------------------------------------------

    int RELOAD = 7;

    int SHUTDOWN = 8;

    //
    // Constants defined from mysql
    //
    // DB Operations
    int SLEEP = 0;

    int STATISTICS = 9;

    int TIME = 15;
}
