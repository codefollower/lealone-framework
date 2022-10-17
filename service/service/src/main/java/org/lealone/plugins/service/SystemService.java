/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.service;

import java.util.ArrayList;
import java.util.Map;

import org.lealone.common.util.CamelCaseHelper;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.service.Service;
import org.lealone.db.service.ServiceMethod;
import org.lealone.db.table.Column;
import org.lealone.plugins.orm.json.JsonArray;
import org.lealone.plugins.orm.json.JsonObject;

public class SystemService {

    public static String execute(String serviceName, String json) {
        serviceName = serviceName.toUpperCase();
        String[] a = StringUtils.arraySplit(serviceName, '.');
        if (a.length == 4) {
            return execute(a[0], a[1], a[3], json);
        } else {
            throw new RuntimeException("service " + serviceName + " not found");
        }
    }

    public static String execute(String serviceName, String methodName, Map<String, Object> methodArgs) {
        serviceName = serviceName.toUpperCase();
        methodName = methodName.toUpperCase();
        String[] a = StringUtils.arraySplit(serviceName, '.');
        if (a.length == 3) {
            return execute(a[0], a[1], methodName, methodArgs);
        } else {
            throw new RuntimeException("service " + serviceName + " not found");
        }
    }

    public static String execute(String dbName, String schemaName, String methodName, String json) {
        switch (methodName) {
        case "LOAD_SERVICES":
            JsonArray ja = new JsonArray(json);
            String serviceNames = ja.getString(0);
            return loadServices(dbName, schemaName, serviceNames);
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }

    public static String execute(String dbName, String schemaName, String methodName,
            Map<String, Object> methodArgs) {
        switch (methodName) {
        case "LOAD_SERVICES":
            Object serviceNames = methodArgs.get("serviceNames");
            if (serviceNames == null)
                return "";
            return loadServices(dbName, schemaName, serviceNames.toString());
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }

    public static String loadServices(String dbName, String schemaName, String serviceNames) {
        JsonArray ja = new JsonArray();
        Database db = LealoneDatabase.getInstance().getDatabase(dbName);
        for (String serviceName : serviceNames.split(",")) {
            Service service = Service.getService(null, db, schemaName, serviceName);
            JsonArray jaMethods = new JsonArray();
            for (ServiceMethod serviceMethod : service.getServiceMethods()) {
                ArrayList<String> parameterNames = new ArrayList<>(serviceMethod.getParameters().size());
                for (Column c : serviceMethod.getParameters()) {
                    parameterNames.add(CamelCaseHelper.toCamelFromUnderscore(c.getName()));
                }
                JsonObject methodJson = new JsonObject();
                methodJson.put("methodName", CamelCaseHelper
                        .toCamelFromUnderscore(serviceMethod.getMethodName().toLowerCase()));
                if (parameterNames.isEmpty())
                    methodJson.put("parameterNames", new JsonArray());
                else
                    methodJson.put("parameterNames", new JsonArray(parameterNames));
                jaMethods.add(methodJson);
            }
            JsonObject serviceJson = new JsonObject();
            serviceJson.put("serviceName", serviceName);
            serviceJson.put("serviceMethods", jaMethods);
            ja.add(serviceJson);
        }
        return ja.encode();
    }
}
