/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.polyglot.test;

import java.io.File;
import java.io.IOException;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;

//-truffle -XX:+IgnoreUnrecognizedVMOptions -javaagent -Djdk.internal.vm.ci.enabled=true

public class HelloPolyglot {

    public static void test() {
        System.out.println("Hello test!");
    }

    public static void main(String[] args) {
        System.out.println("Hello Java!");
        try (Context context = Context.create()) {
            context.eval("js", "print('Hello JavaScript!');");
        }

        try (Context context = Context.create()) {
            Value function = context.eval("js", "x => x+1");
            assert function.canExecute();
            int x = function.execute(41).asInt();
            assert x == 42;
        }

        try (Context context = Context.create()) {
            Value result = context.eval("js", "({ " + //
                    "id   : 42, " + //
                    "text : '42', " + //
                    "arr  : [1,42,3] " + //
                    "})");
            assert result.hasMembers();

            int id = result.getMember("id").asInt();
            assert id == 42;

            String text = result.getMember("text").asString();
            assert text.equals("42");

            Value array = result.getMember("arr");
            assert array.hasArrayElements();
            assert array.getArraySize() == 3;
            assert array.getArrayElement(1).asInt() == 42;
        }

        try (Context context = Context.create()) {
            Source source;
            try {
                source = Source.newBuilder("js", new File("src/test/resources/js/my-source.js")).build();
                Value result = context.eval(source);
                result = result.execute(42);
                assert result.asInt() == 43;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try (Context context = Context.create()) {
            Source source;
            try {
                source = Source.newBuilder("js", new File("src/test/resources/js/my-polyglot.js")).build();
                Value result = context.eval(source);
                result.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try (Context context = Context.newBuilder("js").allowHostClassLookup(s -> true).allowHostAccess(HostAccess.ALL)
                .build()) {
            Source source;
            try {
                source = Source.newBuilder("js", new File("src/test/resources/js/my-polyglot.js")).build();
                context.eval(source);
                // Value result = context.eval(source);
                // result.execute();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try (Context context = Context.newBuilder("js").allowHostClassLookup(s -> true).allowHostAccess(HostAccess.ALL)
                .build()) {
            Source source;
            try {
                source = Source.newBuilder("js", new File("src/test/resources/js/my-app.js")).build();
                context.eval(source);
                // Value result = context.eval(source);
                // result.execute();
            } catch (Exception e) {
                // e.printStackTrace();
            }
        }
    }
}
