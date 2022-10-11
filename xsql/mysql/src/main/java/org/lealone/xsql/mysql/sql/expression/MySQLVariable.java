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
package org.lealone.xsql.mysql.sql.expression;

import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.sql.expression.Variable;

public class MySQLVariable extends Variable {

    public MySQLVariable(ServerSession session, String name) {
        super(session, name);
    }

    @Override
    public Value getValue(ServerSession session) {
        switch (getName().toLowerCase()) {
        case "max_allowed_packet":
        case "net_buffer_length":
            return ValueInt.get(-1);
        case "auto_increment_increment":
            return ValueInt.get(1);
        }
        return super.getValue(session);
    }
}
