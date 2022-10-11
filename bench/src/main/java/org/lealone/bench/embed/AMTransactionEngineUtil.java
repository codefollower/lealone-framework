/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.embed;

import java.util.HashMap;
import java.util.Map;

import org.lealone.db.Constants;
import org.lealone.db.PluginManager;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngine;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.aote.log.LogSyncService;

public class AMTransactionEngineUtil {

    public static Storage getStorage() {
        StorageEngine se = PluginManager.getPlugin(StorageEngine.class, Constants.DEFAULT_STORAGE_ENGINE_NAME);

        StorageBuilder storageBuilder = se.getStorageBuilder();
        storageBuilder.storagePath(TestBase.joinDirs("amte", "data"));
        Storage storage = storageBuilder.openStorage();
        return storage;
    }

    public static TransactionEngine getTransactionEngine() {
        return getTransactionEngine(null, false);
    }

    public static TransactionEngine getTransactionEngine(boolean isDistributed) {
        return getTransactionEngine(null, isDistributed);
    }

    public static TransactionEngine getTransactionEngine(Map<String, String> config) {
        return getTransactionEngine(config, false);
    }

    public static TransactionEngine getTransactionEngine(Map<String, String> config, boolean isDistributed) {
        TransactionEngine te = PluginManager.getPlugin(TransactionEngine.class,
                Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
        if (config == null)
            config = getDefaultConfig();
        if (isDistributed) {
            config.put("host_and_port", Constants.DEFAULT_HOST + ":" + Constants.DEFAULT_TCP_PORT);
        }
        te.init(config);
        return te;
    }

    public static Map<String, String> getDefaultConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("base_dir", TestBase.joinDirs("aote"));
        config.put("redo_log_dir", "redo_log");
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_INSTANT);
        // config.put("checkpoint_service_loop_interval", "10"); // 10ms
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        // config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_NO_SYNC);
        config.put("log_sync_period", "500"); // 500ms
        return config;
    }
}
