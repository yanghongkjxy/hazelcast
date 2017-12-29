package com.hazelcast.dataset.impl;

import com.hazelcast.config.DataSetConfig;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class DataSetContainer {

    private final DataSetStore[] recordStores;
    private final DataSetConfig config;
    private final NodeEngineImpl nodeEngine;
    private final Compiler compiler = new Compiler();

    public DataSetContainer(DataSetConfig config, NodeEngineImpl nodeEngine) {
        this.config = config;
        this.nodeEngine = nodeEngine;
        this.recordStores = new DataSetStore[nodeEngine.getPartitionService().getPartitionCount()];
    }

    public DataSetStore getRecordStore(int partitionId) {
        DataSetStore recordStore = recordStores[partitionId];
        if (recordStore == null) {
            recordStore = new DataSetStore(config, nodeEngine.getSerializationService(),compiler);
            recordStores[partitionId] = recordStore;
        }
        return recordStore;
    }
}
