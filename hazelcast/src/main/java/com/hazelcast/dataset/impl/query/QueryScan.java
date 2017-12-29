package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.Scan;

public abstract class QueryScan extends Scan {

    public abstract void run();
}
