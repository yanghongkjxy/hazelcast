package com.hazelcast.dataset.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.SimpleMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class DataSetService implements ManagedService, RemoteService {
    public static final String SERVICE_NAME = "hz:impl:simpleMapService";
    private final ConcurrentMap<String, DataSetContainer> containers = new ConcurrentHashMap<String, DataSetContainer>();

    private final ConstructorFunction<String, DataSetContainer> containerConstructorFunction =
            new ConstructorFunction<String, DataSetContainer>() {
                public DataSetContainer createNew(String key) {
                    Config config = nodeEngine.getConfig();
                    SimpleMapConfig simpleMapConfig = config.findSimpleMapConfig(key);
                    return new DataSetContainer(simpleMapConfig, nodeEngine);
                }
            };
    private NodeEngineImpl nodeEngine;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    public DataSetContainer getDataSetContainer(String name) {
        return getOrPutIfAbsent(containers, name, containerConstructorFunction);
    }


    @Override
    public DistributedObject createDistributedObject(String name) {
        return new com.hazelcast.dataset.impl.SimpleMapProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {

    }
}
