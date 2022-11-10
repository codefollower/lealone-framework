package org.lealone.plugins.test.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.plugins.test.service.generated.CollectionTypeService;

public class CollectionTypeServiceImpl implements CollectionTypeService {

    @Override
    public List<Object> m1() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(100);
        list.add(200);
        return list;
    }

    @Override
    public List<Integer> m2() {
        return null;
    }

    @Override
    public Set<Object> m3() {
        return null;
    }

    @Override
    public Set<String> m4() {
        HashSet<String> set = new HashSet<>();
        set.add("abc");
        set.add("def");
        return set;
    }

    @Override
    public Map<Object, Object> m5() {
        return null;
    }

    @Override
    public Map<Integer, String> m6() {
        HashMap<Integer, String> map = new HashMap<>();
        map.put(10, "a");
        map.put(20, "b");
        return map;
    }

    @Override
    public Map<Integer, String> m7(List<Integer> p1, Set<String> p2, Map<Integer, String> p3,
            Integer p4) {
        return null;
    }
}
