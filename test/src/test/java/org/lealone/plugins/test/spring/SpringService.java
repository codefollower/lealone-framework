/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.test.spring;

//用这样的url打开: 
//http://localhost:8080/service/spring_service/test?name=zhh
public class SpringService {
    public String test(String name) {
        return String.format("Hello %s!", name);
    }
}
