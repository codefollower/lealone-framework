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
package org.lealone.plugins.test.vertx;

import io.vertx.core.file.impl.FileResolver;
import io.vertx.core.spi.resolver.ResolverProvider;

public class VertxTestBase extends org.lealone.test.TestBase {

    static {
        setVertxProperties();
    }

    public VertxTestBase() {
        optimizeVertx();
    }

    public static void optimizeVertx() {
        // 如果不禁用的话
        // 执行到javax.naming.spi.NamingManager.getInitialContext方法的return factory.getInitialContext()会
        // 生成一个类似"Thread-XXX"这样的线程
        System.setProperty(ResolverProvider.DISABLE_DNS_RESOLVER_PROP_NAME, "true");

        // disableSSLContext();
    }

    // @SuppressWarnings("restriction")
    // private static void disableSSLContext() {
    // // 这一行可以屏蔽在io.vertx.core.net.impl.SSLHelper类的static初始代码中调用耗时的SSLContext.getInstance("TLS")
    // // 但是在JDK 1.8.0_112-b15中不能正常启动，
    // // 在我自己构建的openjdk8-b132中就可以正常启动(能减少700ms以上)
    // String version = ManagementFactory.getRuntimeMXBean().getVmVersion();
    // if (version.equals("25.71-b00-fastdebug")) {
    // sun.security.jca.Providers.setProviderList(null);
    // }
    // // sun.security.jca.Providers.setProviderList(Providers.getProviderList());
    // }

    private static void setVertxProperties() {
        System.setProperty(FileResolver.DISABLE_FILE_CACHING_PROP_NAME, "true");
        System.setProperty(FileResolver.DISABLE_CP_RESOLVING_PROP_NAME, "true");
        System.setProperty(FileResolver.CACHE_DIR_BASE_PROP_NAME, "./" + TEST_DIR + "/.vertx");
    }

}
