/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.polyglot.python;

import java.io.File;

import org.lealone.db.PluginBase;
import org.lealone.db.service.Service;
import org.lealone.db.service.ServiceExecutorFactory;

public class PythonServiceExecutorFactory extends PluginBase implements ServiceExecutorFactory {

    public PythonServiceExecutorFactory() {
        super("python");
    }

    @Override
    public PythonServiceExecutor createServiceExecutor(Service service) {
        return new PythonServiceExecutor(service);
    }

    @Override
    public boolean supportsGenCode() {
        return true;
    }

    @Override
    public void genCode(Service service) {
        if (new File(service.getImplementBy()).exists()) {
            return;
        }
    }
}
