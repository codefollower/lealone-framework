package org.lealone.plugins.test.service.generated.executor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lealone.db.service.ServiceExecutor;
import org.lealone.db.value.*;
import org.lealone.plugins.orm.json.JsonArray;
import org.lealone.plugins.test.service.impl.CollectionTypeServiceImpl;

/**
 * Service executor for 'collection_type_service'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class CollectionTypeServiceExecutor implements ServiceExecutor {

    private final CollectionTypeServiceImpl s = new CollectionTypeServiceImpl();

    @Override
    public Value executeService(String methodName, Value[] methodArgs) {
        switch (methodName) {
        case "M1":
            List<Object> result1 = this.s.m1();
            if (result1 == null)
                return ValueNull.INSTANCE;
            return ValueList.get(Object.class, result1);
        case "M2":
            List<Integer> result2 = this.s.m2();
            if (result2 == null)
                return ValueNull.INSTANCE;
            return ValueList.get(Integer.class, result2);
        case "M3":
            Set<Object> result3 = this.s.m3();
            if (result3 == null)
                return ValueNull.INSTANCE;
            return ValueSet.get(Object.class, result3);
        case "M4":
            Set<String> result4 = this.s.m4();
            if (result4 == null)
                return ValueNull.INSTANCE;
            return ValueSet.get(String.class, result4);
        case "M5":
            Map<Object, Object> result5 = this.s.m5();
            if (result5 == null)
                return ValueNull.INSTANCE;
            return ValueMap.get(Object.class, Object.class, result5);
        case "M6":
            Map<Integer, String> result6 = this.s.m6();
            if (result6 == null)
                return ValueNull.INSTANCE;
            return ValueMap.get(Integer.class, String.class, result6);
        case "M7":
            List<Integer> p_p1_7 = methodArgs[0].getCollection();
            Set<String> p_p2_7 = methodArgs[1].getCollection();
            Map<Integer, String> p_p3_7 = methodArgs[2].getCollection();
            Integer p_p4_7 = methodArgs[3].getInt();
            Map<Integer, String> result7 = this.s.m7(p_p1_7, p_p2_7, p_p3_7, p_p4_7);
            if (result7 == null)
                return ValueNull.INSTANCE;
            return ValueMap.get(Integer.class, String.class, result7);
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }

    @Override
    public String executeService(String methodName, Map<String, Object> methodArgs) {
        switch (methodName) {
        case "M1":
            List<Object> result1 = this.s.m1();
            if (result1 == null)
                return null;
            return result1.toString();
        case "M2":
            List<Integer> result2 = this.s.m2();
            if (result2 == null)
                return null;
            return result2.toString();
        case "M3":
            Set<Object> result3 = this.s.m3();
            if (result3 == null)
                return null;
            return result3.toString();
        case "M4":
            Set<String> result4 = this.s.m4();
            if (result4 == null)
                return null;
            return result4.toString();
        case "M5":
            Map<Object, Object> result5 = this.s.m5();
            if (result5 == null)
                return null;
            return result5.toString();
        case "M6":
            Map<Integer, String> result6 = this.s.m6();
            if (result6 == null)
                return null;
            return result6.toString();
        case "M7":
            List<Integer> p_p1_7 = ServiceExecutor.toList("P1", methodArgs);
            Set<String> p_p2_7 = ServiceExecutor.toSet("P2", methodArgs);
            Map<Integer, String> p_p3_7 = ServiceExecutor.toMap("P3", methodArgs);
            Integer p_p4_7 = Integer.valueOf(ServiceExecutor.toString("P4", methodArgs));
            Map<Integer, String> result7 = this.s.m7(p_p1_7, p_p2_7, p_p3_7, p_p4_7);
            if (result7 == null)
                return null;
            return result7.toString();
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }

    @Override
    public String executeService(String methodName, String json) {
        JsonArray ja = null;
        switch (methodName) {
        case "M1":
            List<Object> result1 = this.s.m1();
            if (result1 == null)
                return null;
            return result1.toString();
        case "M2":
            List<Integer> result2 = this.s.m2();
            if (result2 == null)
                return null;
            return result2.toString();
        case "M3":
            Set<Object> result3 = this.s.m3();
            if (result3 == null)
                return null;
            return result3.toString();
        case "M4":
            Set<String> result4 = this.s.m4();
            if (result4 == null)
                return null;
            return result4.toString();
        case "M5":
            Map<Object, Object> result5 = this.s.m5();
            if (result5 == null)
                return null;
            return result5.toString();
        case "M6":
            Map<Integer, String> result6 = this.s.m6();
            if (result6 == null)
                return null;
            return result6.toString();
        case "M7":
            ja = new JsonArray(json);
            List<Integer> p_p1_7 = ja.getList(0);
            Set<String> p_p2_7 = ja.getSet(1);
            Map<Integer, String> p_p3_7 = ja.getMap(2, Integer.class);
            Integer p_p4_7 = Integer.valueOf(ja.getValue(3).toString());
            Map<Integer, String> result7 = this.s.m7(p_p1_7, p_p2_7, p_p3_7, p_p4_7);
            if (result7 == null)
                return null;
            return result7.toString();
        default:
            throw new RuntimeException("no method: " + methodName);
        }
    }
}
