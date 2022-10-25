/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.js;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

public class JavaScriptServiceTest {

    public static void main(String[] args) throws Exception {
        try (Context context = Context.create()) {
            Source source;
            try {
                source = Source.newBuilder("js", new File("src/test/resources/js/hello_service.js"))
                        .build();
                context.eval(source);
                Value bindings = context.getBindings("js");
                for (String key : bindings.getMemberKeys()) {
                    System.out.println(key);
                    Value function = bindings.getMember(key);
                    function.canExecute();
                }
                Value function = bindings.getMember("hello");
                String s = function.execute("zhh").asString();
                System.out.println(s);
                s = function.execute("zhh").asString();
                System.out.println(s);
                s = function.execute("zhh").asString();
                System.out.println(s);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        createAndExecuteServices();
    }

    static void createAndExecuteServices() throws Exception {
        String jdbcUrl = "jdbc:lealone:tcp://localhost:9210/lealone?user=root";

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("RUNSCRIPT FROM './src/test/resources/services.sql'");
            ResultSet rs = stmt.executeQuery("EXECUTE SERVICE user_service crud('zhh')");
            rs.next();
            System.out.println(rs.getString(1));
            rs.close();
        }
    }
}
