package org.lealone.plugins.bench.tpcc.codefutures;

public class AbortedTransactionException extends Exception {
    public AbortedTransactionException() {
        super();
    }

    public AbortedTransactionException(String message) {
        super(message);
    }
}
