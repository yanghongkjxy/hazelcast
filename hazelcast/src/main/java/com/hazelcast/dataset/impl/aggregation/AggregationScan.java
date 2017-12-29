package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.dataset.impl.Scan;

public abstract class AggregationScan extends Scan {

    public abstract void run();

    public abstract Aggregator getAggregator();
}
