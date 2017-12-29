package com.hazelcast.dataset.impl;

import com.hazelcast.util.function.Consumer;

public abstract class ProjectionScan extends Scan {

    public Consumer consumer;

    public abstract void run();
}
