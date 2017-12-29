package com.hazelcast.dataset.impl;

import com.hazelcast.config.SimpleMapConfig;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class DataSetContainer {

    private final com.hazelcast.dataset.impl.SimpleRecordStore[] recordStores;
    private final SimpleMapConfig config;
    private final NodeEngineImpl nodeEngine;
    private final Compiler compiler = new Compiler();

    public DataSetContainer(SimpleMapConfig config, NodeEngineImpl nodeEngine) {
        this.config = config;
        this.nodeEngine = nodeEngine;
        this.recordStores = new com.hazelcast.dataset.impl.SimpleRecordStore[nodeEngine.getPartitionService().getPartitionCount()];
    }

    public com.hazelcast.dataset.impl.SimpleRecordStore getRecordStore(int partitionId) {
        com.hazelcast.dataset.impl.SimpleRecordStore recordStore = recordStores[partitionId];
        if (recordStore == null) {
            recordStore = new com.hazelcast.dataset.impl.SimpleRecordStore(config, nodeEngine.getSerializationService(),compiler);
            recordStores[partitionId] = recordStore;
        }
        return recordStore;
    }
}
