/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.dataset.impl;

import com.hazelcast.dataset.impl.aggregation.CompileAggregationOperation;
import com.hazelcast.dataset.impl.aggregation.CompileAggregationOperationFactory;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.dataset.impl.aggregation.AggregateOperation;
import com.hazelcast.dataset.impl.query.CompilePredicateOperation;
import com.hazelcast.dataset.impl.query.CompilePredicateOperationFactory;
import com.hazelcast.dataset.impl.projection.CompileProjectionOperation;
import com.hazelcast.dataset.impl.projection.CompileProjectionOperationFactory;
import com.hazelcast.dataset.impl.operations.SetOperation;
import com.hazelcast.dataset.impl.query.QueryOperationFactory;
import com.hazelcast.dataset.impl.operations.SizeOperation;
import com.hazelcast.dataset.impl.operations.SizeOperationFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY_ID;

public final class DataSetDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(DATA_SET_DS_FACTORY, DATA_SET_DS_FACTORY_ID);

    public static final int SET_OPERATION = 0;
    public static final int COMPILE_PREDICATE_OPERATION = 1;
    public static final int COMPILE_PREDICATE_OPERATION_FACTORY = 2;
    public static final int QUERY_OPERATION =3;
    public static final int QUERY_OPERATION_FACTORY = 4;
    public static final int SIZE_OPERATION = 5;
    public static final int SIZE_OPERATION_FACTORY = 6;
    public static final int COMPILE_PROJECTION_OPERATION = 7;
    public static final int COMPILE_PROJECTION_OPERATION_FACTORY = 8;
    public static final int AGGREGATE_PROJECTION_OPERATION = 9;
    public static final int AGGREGATE_PROJECTION_OPERATION_FACTORY = 10;
    public static final int COMPILE_AGGREGATION = 11;
    public static final int COMPILE_AGGREGATION_OPERATION_FACTORY = 12;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case SET_OPERATION:
                        return new SetOperation();
                    case COMPILE_PREDICATE_OPERATION:
                        return new CompilePredicateOperation();
                    case COMPILE_PREDICATE_OPERATION_FACTORY:
                        return new CompilePredicateOperationFactory();
                    case QUERY_OPERATION:
                        return new QueryOperation();
                    case QUERY_OPERATION_FACTORY:
                        return new QueryOperationFactory();
                    case SIZE_OPERATION:
                        return new SizeOperation();
                    case SIZE_OPERATION_FACTORY:
                        return new SizeOperationFactory();
                    case COMPILE_PROJECTION_OPERATION:
                        return new CompileProjectionOperation();
                    case COMPILE_PROJECTION_OPERATION_FACTORY:
                        return new CompileProjectionOperationFactory();
                    case AGGREGATE_PROJECTION_OPERATION:
                        return new AggregateOperation();
                    case COMPILE_AGGREGATION:
                        return new CompileAggregationOperation();
                    case COMPILE_AGGREGATION_OPERATION_FACTORY:
                        return new CompileAggregationOperationFactory();
                    default:
                        return null;
                }
            }
        };
    }
}
