package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.dataset.impl.Scan;

public abstract class AggregationScan extends Scan {

    public Aggregator aggregator;

    public abstract void run();
}
