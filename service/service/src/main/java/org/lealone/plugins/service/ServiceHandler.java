/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.StringUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.service.Service;
import org.lealone.db.session.ServerSession;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.json.Json;

public class ServiceHandler {

    private static final Logger logger = LoggerFactory.getLogger(ServiceHandler.class);

    protected final String defaultDatabase;
    protected final String defaultSchema;
    protected final ServerSession session;

    public ServiceHandler(Map<String, String> config) {
        defaultDatabase = config.get("default_database");
        defaultSchema = config.get("default_schema");

        String url = config.get("jdbc_url");
        if (url == null)
            url = Constants.URL_PREFIX + Constants.URL_EMBED + defaultDatabase + ";password=;user=root";
        ConnectionInfo ci = new ConnectionInfo(url);
        session = (ServerSession) ci.createSession();
    }

    public ServerSession getSession() {
        return session;
    }

    public String executeService(String serviceName, String methodName, Map<String, Object> methodArgs) {
        return executeService(serviceName, methodName, methodArgs, false);
    }

    public String executeService(String serviceName, String methodName, Map<String, Object> methodArgs,
            boolean disableDynamicCompile) {
        String[] serviceNameArray = StringUtils.arraySplit(serviceName, '.');
        if (serviceNameArray.length == 1 && defaultDatabase != null && defaultSchema != null)
            serviceName = defaultDatabase + "." + defaultSchema + "." + serviceName;
        else if (serviceNameArray.length == 2 && defaultDatabase != null)
            serviceName = defaultDatabase + "." + serviceName;

        Object result = null;
        try {
            logger.info("execute service: {}.{}", serviceName, methodName);
            if (serviceName.toUpperCase().contains("LEALONE_SYSTEM_SERVICE")) {
                result = SystemService.execute(serviceName, methodName, methodArgs);
            } else {
                result = Service.execute(session, serviceName, methodName, methodArgs,
                        disableDynamicCompile);
            }
        } catch (Exception e) {
            result = "failed to execute service: " + serviceName + "." + methodName + ", cause: "
                    + e.getMessage();
            logger.error(result, e);
            // 这种异常还是得抛给调用者
            if (e instanceof RuntimeException)
                throw e;
        }
        // 如果为null就返回"null"字符串
        if (result == null)
            result = "null";

        if (result instanceof List || result instanceof Set || result instanceof Map
                || result instanceof Model) {
            return Json.encode(result);
        }
        return result.toString();
    }
}
