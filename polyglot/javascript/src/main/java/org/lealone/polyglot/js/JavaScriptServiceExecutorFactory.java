/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.polyglot.js;

import java.io.File;

import org.lealone.db.PluginBase;
import org.lealone.db.service.Service;
import org.lealone.db.service.ServiceExecutorFactory;

public class JavaScriptServiceExecutorFactory extends PluginBase implements ServiceExecutorFactory {

    public JavaScriptServiceExecutorFactory() {
        super("js");
    }

    @Override
    public JavaScriptServiceExecutor createServiceExecutor(Service service) {
        return new JavaScriptServiceExecutor(service);
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
