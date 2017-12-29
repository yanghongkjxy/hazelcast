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

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.dataset.impl.operations.AggregateOperation;
import com.hazelcast.dataset.impl.operations.CompilePredicateOperation;
import com.hazelcast.dataset.impl.operations.CompilePredicateOperationFactory;
import com.hazelcast.dataset.impl.operations.CompileProjectionOperation;
import com.hazelcast.dataset.impl.operations.CompileProjectionOperationFactory;
import com.hazelcast.dataset.impl.operations.SetOperation;
import com.hazelcast.dataset.impl.operations.QueryOperationFactory;
import com.hazelcast.dataset.impl.operations.SizeOperation;
import com.hazelcast.dataset.impl.operations.SizeOperationFactory;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.DATA_SET_DS_FACTORY_ID;

public final class DataSetDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(DATA_SET_DS_FACTORY, DATA_SET_DS_FACTORY_ID);

    public static final int SET = 0;
    public static final int COMPILE_PREDICATE = 1;
    public static final int COMPILE_PREDICATE_OPERATION_FACTORY = 2;
    public static final int QUERY =3;
    public static final int QUERY_OPERATION_FACTORY = 4;
    public static final int SIZE = 5;
    public static final int SIZE_OPERATION_FACTORY = 6;
    public static final int COMPILE_PROJECTION = 7;
    public static final int COMPILE_PROJECTION_OPERATION_FACTORY = 8;
    public static final int AGGREGATE_PROJECTION = 9;
    public static final int AGGREGATE_PROJECTION_OPERATION_FACTORY = 10;

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
                    case SET:
                        return new SetOperation();
                    case COMPILE_PREDICATE:
                        return new CompilePredicateOperation();
                    case COMPILE_PREDICATE_OPERATION_FACTORY:
                        return new CompilePredicateOperationFactory();
                    case QUERY:
                        return new QueryOperation();
                    case QUERY_OPERATION_FACTORY:
                        return new QueryOperationFactory();
                    case SIZE:
                        return new SizeOperation();
                    case SIZE_OPERATION_FACTORY:
                        return new SizeOperationFactory();
                    case COMPILE_PROJECTION:
                        return new CompileProjectionOperation();
                    case COMPILE_PROJECTION_OPERATION_FACTORY:
                        return new CompileProjectionOperationFactory();
                    case AGGREGATE_PROJECTION:
                        return new AggregateOperation();
                    default:
                        return null;
                }
            }
        };
    }
}
