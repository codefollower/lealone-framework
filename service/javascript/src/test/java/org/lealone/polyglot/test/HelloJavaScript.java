/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.polyglot.test;

import org.graalvm.polyglot.Context;

public class HelloJavaScript {

    public static void main(String[] args) {
        try (Context context = Context.create()) {
            context.eval("js", "print('Hello JavaScript!');");
        }
    }

}
