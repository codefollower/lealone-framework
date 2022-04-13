/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.test.memory;

import org.junit.Test;
import org.lealone.test.TestBase;
import org.lealone.test.misc.CRUDExample;

public class MemoryStorageTest extends TestBase {
    @Test
    public void run() throws Exception {
        TestBase test = new TestBase();
        test.setStorageEngineName(org.lealone.plugins.memory.MemoryStorageEngine.NAME);
        test.setInMemory(true);
        test.setEmbedded(true);
        test.printURL();
        CRUDExample.crud(test.getConnection());
    }
}
