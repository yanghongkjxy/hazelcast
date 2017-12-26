package com.hazelcast.simplemap.impl;

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

public class SimpleMapService implements ManagedService, RemoteService {
    public static final String SERVICE_NAME = "hz:impl:simpleMapService";
    private final ConcurrentMap<String, SimpleMapContainer> containers = new ConcurrentHashMap<String, SimpleMapContainer>();

    private final ConstructorFunction<String, SimpleMapContainer> containerConstructorFunction =
            new ConstructorFunction<String, SimpleMapContainer>() {
                public SimpleMapContainer createNew(String key) {
                    Config config = nodeEngine.getConfig();
                    SimpleMapConfig simpleMapConfig = config.findSimpleMapConfig(key);
                    return new SimpleMapContainer(simpleMapConfig, nodeEngine);
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

    public SimpleMapContainer getSimpleMapContainer(String name) {
        return getOrPutIfAbsent(containers, name, containerConstructorFunction);
    }


    @Override
    public DistributedObject createDistributedObject(String name) {
        return new SimpleMapProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {

    }
}
