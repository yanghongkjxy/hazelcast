package com.hazelcast.simplemap.impl;

import sun.misc.Unsafe;

public abstract class FullTableScan {

    public Unsafe unsafe;
    public long slabPointer;
    public long recordDataSize;
    public long recordIndex;

    public abstract void run();
}
