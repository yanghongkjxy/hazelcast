/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.semaphore.operation;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphore;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftOp;

import java.io.IOException;

/**
 * Operation for {@link ISemaphore#availablePermits()}
 *
 * @see RaftSemaphore#getAvailable()
 */
public class AvailablePermitsOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private String name;

    public AvailablePermitsOp() {
    }

    public AvailablePermitsOp(String name) {
        this.name = name;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftSemaphoreService service = getService();
        return service.availablePermits(groupId, name);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    protected String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.AVAILABLE_PERMITS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
    }
}
