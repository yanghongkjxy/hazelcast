package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.Scan;

import java.util.List;

public abstract class QueryScan extends Scan {

    public abstract List run();
}
