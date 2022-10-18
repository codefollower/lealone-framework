/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.test.spring;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class SpringServiceTest extends SqlTestBase {

    public SpringServiceTest() {
        super(LealoneDatabase.NAME);
        setEmbedded(true);
    }

    @Test
    public void createService() {
        executeUpdate("drop service if exists spring_service");
        String sql = "create service if not exists spring_service (" //
                + " test(name varchar) varchar)" //
                + " implement by '" + SpringService.class.getName() + "'";
        executeUpdate(sql);
    }
}
