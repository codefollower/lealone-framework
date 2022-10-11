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

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;

/**
 * 字段类型及标识定义
 * 
 * @author xianmao.hexm
 * @author zhh
 */
public interface Fields {

    /** field data type */
    int FIELD_TYPE_DECIMAL = 0;
    int FIELD_TYPE_TINY = 1;
    int FIELD_TYPE_SHORT = 2;
    int FIELD_TYPE_LONG = 3;
    int FIELD_TYPE_FLOAT = 4;
    int FIELD_TYPE_DOUBLE = 5;
    int FIELD_TYPE_NULL = 6;
    int FIELD_TYPE_TIMESTAMP = 7;
    int FIELD_TYPE_LONGLONG = 8;
    int FIELD_TYPE_INT24 = 9;
    int FIELD_TYPE_DATE = 10;
    int FIELD_TYPE_TIME = 11;
    int FIELD_TYPE_DATETIME = 12;
    int FIELD_TYPE_YEAR = 13;
    int FIELD_TYPE_NEWDATE = 14;
    int FIELD_TYPE_VARCHAR = 15;
    int FIELD_TYPE_BIT = 16;
    int FIELD_TYPE_NEW_DECIMAL = 246;
    int FIELD_TYPE_ENUM = 247;
    int FIELD_TYPE_SET = 248;
    int FIELD_TYPE_TINY_BLOB = 249;
    int FIELD_TYPE_MEDIUM_BLOB = 250;
    int FIELD_TYPE_LONG_BLOB = 251;
    int FIELD_TYPE_BLOB = 252;
    int FIELD_TYPE_VAR_STRING = 253;
    int FIELD_TYPE_STRING = 254;
    int FIELD_TYPE_GEOMETRY = 255;

    /** field flag */
    int NOT_NULL_FLAG = 0x0001;
    int PRI_KEY_FLAG = 0x0002;
    int UNIQUE_KEY_FLAG = 0x0004;
    int MULTIPLE_KEY_FLAG = 0x0008;
    int BLOB_FLAG = 0x0010;
    int UNSIGNED_FLAG = 0x0020;
    int ZEROFILL_FLAG = 0x0040;
    int BINARY_FLAG = 0x0080;
    int ENUM_FLAG = 0x0100;
    int AUTO_INCREMENT_FLAG = 0x0200;
    int TIMESTAMP_FLAG = 0x0400;
    int SET_FLAG = 0x0800;

    public static int toMySQLType(int valueType) {
        switch (valueType) {
        case Value.BOOLEAN:
            return FIELD_TYPE_TINY;
        case Value.BYTE:
            return FIELD_TYPE_TINY;
        case Value.SHORT:
            return FIELD_TYPE_SHORT;
        case Value.INT:
            return FIELD_TYPE_LONG;
        case Value.LONG:
            return FIELD_TYPE_LONGLONG;
        case Value.DECIMAL:
            return FIELD_TYPE_DECIMAL;
        case Value.TIME:
            return FIELD_TYPE_TIME;
        case Value.DATE:
            return FIELD_TYPE_DATE;
        case Value.TIMESTAMP:
            return FIELD_TYPE_TIMESTAMP;
        case Value.BYTES:
        case Value.UUID:
            return FIELD_TYPE_BLOB;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            return FIELD_TYPE_STRING;
        case Value.BLOB:
            return FIELD_TYPE_BLOB;
        case Value.CLOB:
            return FIELD_TYPE_STRING;
        case Value.DOUBLE:
            return FIELD_TYPE_DOUBLE;
        case Value.FLOAT:
            return FIELD_TYPE_FLOAT;
        case Value.NULL:
            return FIELD_TYPE_STRING;
        case Value.JAVA_OBJECT:
            return FIELD_TYPE_STRING;
        case Value.UNKNOWN:
            // anything
            return FIELD_TYPE_STRING;
        case Value.ARRAY:
            return FIELD_TYPE_STRING;
        case Value.RESULT_SET:
            return FIELD_TYPE_STRING;
        default:
            throw DbException.getInternalError("type=" + valueType);
        }
    }
}
