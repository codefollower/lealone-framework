/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.polyglot.test;

import java.io.File;
import java.io.IOException;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

public class JavaScriptServiceTest {

    public static void main(String[] args) {
        try (Context context = Context.create()) {
            Source source;
            try {
                source = Source.newBuilder("js", new File("src/test/resources/js/hello_service.js")).build();
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
    }

}
