package org.lealone.plugins.test.service.demo.generated.executor;

import io.vertx.core.json.JsonArray;
import org.lealone.db.service.ServiceExecutor;
import org.lealone.plugins.test.service.demo.DemoServiceImpl;

/**
 * Service executor for 'demo_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class DemoServiceExecutor implements ServiceExecutor {

    private final DemoServiceImpl s = new DemoServiceImpl();

    public DemoServiceExecutor() {
    }

    @Override
    public String executeService(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "SAY_HELLO":
            ja = new JsonArray(json);
            String p_name1 = ja.getString(0);
            String result1 = this.s.sayHello(p_name1);
            if (result1 == null)
                return null;
            return result1;
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }
}
