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
package org.lealone.xsql.mysql.sql;

import org.lealone.db.CommandParameter;
import org.lealone.db.PluginBase;
import org.lealone.db.schema.Sequence;
import org.lealone.db.session.Session;
import org.lealone.db.value.Value;
import org.lealone.sql.IExpression;
import org.lealone.sql.SQLEngine;
import org.lealone.sql.SQLParser;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.SequenceValue;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.condition.ConditionAndOr;
import org.lealone.xsql.mysql.server.MySQLServerEngine;

public class MySQLEngine extends PluginBase implements SQLEngine {

    public MySQLEngine() {
        super(MySQLServerEngine.NAME);
    }

    @Override
    public SQLParser createParser(Session session) {
        return new MySQLParser((org.lealone.db.session.ServerSession) session);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return MySQLParser.quoteIdentifier(identifier);
    }

    @Override
    public CommandParameter createParameter(int index) {
        return new Parameter(index);
    }

    @Override
    public IExpression createValueExpression(Value value) {
        return ValueExpression.get(value);
    }

    @Override
    public IExpression createSequenceValue(Object sequence) {
        return new SequenceValue((Sequence) sequence);
    }

    @Override
    public IExpression createConditionAndOr(boolean and, IExpression left, IExpression right) {
        return new ConditionAndOr(and ? ConditionAndOr.AND : ConditionAndOr.OR,
                (org.lealone.sql.expression.Expression) left,
                (org.lealone.sql.expression.Expression) right);
    }
}
