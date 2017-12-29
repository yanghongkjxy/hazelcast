package com.hazelcast.dataset.impl.projection;

import com.hazelcast.dataset.impl.Scan;
import com.hazelcast.util.function.Consumer;

public abstract class ProjectionScan extends Scan {

    public Consumer consumer;

    public abstract void run();
}
