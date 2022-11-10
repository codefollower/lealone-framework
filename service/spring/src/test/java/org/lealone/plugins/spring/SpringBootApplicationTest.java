/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.plugins.spring;

import java.sql.Connection;
import java.sql.DriverManager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
@ComponentScan(basePackages = "org.lealone.plugins.spring")
public class SpringBootApplicationTest {

    public static void main(String[] args) throws Exception {
        createService();
        SpringApplication.run(SpringBootApplicationTest.class, args);
    }

    public static void createService() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:lealone:embed:lealone", "root", "");
        String sql = "create service if not exists spring_service (test(name varchar) varchar)" //
                + " implement by '" + SpringBootApplicationTest.class.getName() + "'";
        conn.createStatement().executeUpdate(sql);
        conn.close();
    }

    // 用这样的url打开: http://localhost:8080/hello?name=zhh
    @GetMapping("/hello")
    public String hello(@RequestParam(value = "name", defaultValue = "World") String name) {
        return String.format("Hello %s!", name);
    }

    // 用这样的url打开: http://localhost:8080/service/spring_service/test?name=zhh
    public String test(String name) {
        return String.format("Hello %s!", name);
    }
}
