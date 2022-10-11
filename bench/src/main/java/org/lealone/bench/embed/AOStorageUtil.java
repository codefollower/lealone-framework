/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.embed;

import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.AOStorageBuilder;

public class AOStorageUtil {

    public static AOStorage openStorage() {
        AOStorageBuilder builder = new AOStorageBuilder();
        return openStorage(builder);
    }

    public static AOStorage openStorage(int pageSplitSize) {
        AOStorageBuilder builder = new AOStorageBuilder();
        builder.pageSplitSize(pageSplitSize);
        return openStorage(builder);
    }

    public static AOStorage openStorage(int pageSplitSize, int cacheSize) {
        AOStorageBuilder builder = new AOStorageBuilder();
        builder.pageSplitSize(pageSplitSize);
        builder.cacheSize(cacheSize);
        return openStorage(builder);
    }

    public static AOStorage openStorage(AOStorageBuilder builder) {
        String storagePath = TestBase.joinDirs("aose");
        builder.compressHigh();
        builder.storagePath(storagePath).minFillRate(30);
        AOStorage storage = builder.openStorage();
        return storage;
    }
}
