package com.hazelcast.simplemap.impl;

import sun.misc.Unsafe;

import java.util.Map;

public abstract class FullTableScan {

    public Unsafe unsafe;
    public long slabPointer;
    public long recordDataSize;
    public long recordIndex;

    public abstract void init(Map<String,Object> bindings);

    public abstract void run();
}
