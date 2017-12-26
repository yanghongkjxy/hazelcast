package com.hazelcast.simplemap.impl;

import com.hazelcast.config.SimpleMapConfig;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class SimpleMapContainer {

    private final SimpleRecordStore[] recordStores;
    private final SimpleMapConfig config;
    private final NodeEngineImpl nodeEngine;
    private final FullTableScanCompiler compiler = new FullTableScanCompiler();

    public SimpleMapContainer(SimpleMapConfig config, NodeEngineImpl nodeEngine) {
        this.config = config;
        this.nodeEngine = nodeEngine;
        this.recordStores = new SimpleRecordStore[nodeEngine.getPartitionService().getPartitionCount()];
    }

    public SimpleRecordStore getRecordStore(int partitionId) {
        SimpleRecordStore recordStore = recordStores[partitionId];
        if (recordStore == null) {
            recordStore = new SimpleRecordStore(config, nodeEngine.getSerializationService(),compiler);
            recordStores[partitionId] = recordStore;
        }
        return recordStore;
    }
}
