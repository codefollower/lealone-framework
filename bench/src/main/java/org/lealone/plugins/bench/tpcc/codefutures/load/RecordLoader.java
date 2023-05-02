package org.lealone.plugins.bench.tpcc.codefutures.load;

/**
 * Copyright (C) 2011 CodeFutures Corporation. All rights reserved.
 */
public interface RecordLoader {

    void load(Record r) throws Exception;

    void commit() throws Exception;

    void close() throws Exception;
}
