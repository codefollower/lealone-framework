/*
 * Copyright Lealone Database Group. CodeFutures Corporation
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh, CodeFutures Corporation
 */
package org.lealone.plugins.bench.tpcc.codefutures.bench;

public class AbortedTransactionException extends Exception {
    public AbortedTransactionException() {
        super();
    }

    public AbortedTransactionException(String message) {
        super(message);
    }
}
