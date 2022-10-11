/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.write.singleRowUpdate;

import java.sql.Connection;
import java.sql.Statement;

public class H2UpdateBTest extends UpdateBTest {

    public static void main(String[] args) throws Throwable {
        Connection conn = getConnection(9511, "test", "test");
        Statement statement = conn.createStatement();

        UpdateBTest.run("H2Update", statement);
        statement.close();
        conn.close();
    }
}
