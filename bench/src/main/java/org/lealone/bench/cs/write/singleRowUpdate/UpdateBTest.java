/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.bench.cs.write.singleRowUpdate;

import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.lealone.bench.cs.ClientServerBTest;

//-XX:+UnlockExperimentalVMOptions -XX:+UseZGC -Xmx800M
public abstract class UpdateBTest extends ClientServerBTest {

    public static void run(String name, Statement statement) throws Throwable {
        initData(statement);
        String sql = "update test set f1=2 where name='abc1'";
        int count = 1000;
        for (int i = 0; i < count * 5; i++)
            statement.executeUpdate(sql);

        int loop = 20;
        for (int j = 0; j < loop; j++) {
            long t1 = System.nanoTime();
            for (int i = 0; i < count; i++)
                statement.executeUpdate(sql);
            long t2 = System.nanoTime();
            System.out.println(name + ": " + TimeUnit.NANOSECONDS.toMicros(t2 - t1) / count);
        }
        System.out.println();
        System.out.println("time: 微秒");
        System.out.println("loop: " + loop + " * " + count);
        System.out.println("sql : " + sql);
    }
}
