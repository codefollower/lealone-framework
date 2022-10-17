/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.service.http;

import java.util.Map;

public interface HttpRouter {
    void init(HttpServer server, Map<String, String> config);
}
