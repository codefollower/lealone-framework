/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.polyglot.python;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Source;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.db.service.Service;
import org.lealone.db.service.ServiceExecutorBase;
import org.lealone.db.service.ServiceMethod;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;

public class PythonServiceExecutor extends ServiceExecutorBase {

    private final Map<String, org.graalvm.polyglot.Value> functionMap;
    private Context context;

    public PythonServiceExecutor(Service service) {
        int size = service.getServiceMethods().size();
        serviceMethodMap = new HashMap<>(size);
        functionMap = new HashMap<>(size);

        Source source;
        org.graalvm.polyglot.Value bindings;
        try {
            context = Context.newBuilder().allowIO(true).allowHostAccess(HostAccess.ALL)
                    // allows access to all Java classes
                    .allowHostClassLookup(className -> true).build();
            source = Source.newBuilder("python", new File(service.getImplementBy())).build();
            context.eval(source);
            bindings = context.getBindings("python");
        } catch (IOException e) {
            throw DbException.convert(e);
        }

        for (ServiceMethod serviceMethod : service.getServiceMethods()) {
            String serviceMethodName = serviceMethod.getMethodName();
            serviceMethodMap.put(serviceMethodName, serviceMethod);

            String functionName = CamelCaseHelper.toCamelFromUnderscore(serviceMethodName);
            try {
                functionMap.put(serviceMethodName, bindings.getMember(functionName));
            } catch (Exception e) {
                throw new RuntimeException("Function not found: " + functionName, e);
            }
        }
    }

    @Override
    public Value executeService(String methodName, Value[] methodArgs) {
        Object[] args = getServiceMethodArgs(methodName, methodArgs);
        org.graalvm.polyglot.Value function = functionMap.get(methodName);
        try {
            String ret = function.execute(args).toString();
            if (ret == null)
                return ValueNull.INSTANCE;
            return ValueString.get(ret);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String executeService(String methodName, Map<String, Object> methodArgs) {
        Object[] args = getServiceMethodArgs(methodName, methodArgs);
        org.graalvm.polyglot.Value function = functionMap.get(methodName);
        try {
            String ret = function.execute(args).toString();
            if (ret == null)
                return null;
            return ret;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String executeService(String methodName, String json) {
        Object[] args = getServiceMethodArgs(methodName, json);
        org.graalvm.polyglot.Value function = functionMap.get(methodName);
        try {
            String ret = function.execute(args).toString();
            if (ret == null)
                return null;
            return ret;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }
}
