package com.hazelcast.dataset.impl;

import sun.misc.Unsafe;

import java.util.Map;

public abstract class Scan {

    public Unsafe unsafe;
    public long slabPointer;
    public long recordDataSize;
    public long recordIndex;

    public abstract void init(Map<String,Object> bindings);

}
