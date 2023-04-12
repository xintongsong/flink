/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.cache.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common.TieredStoreProducer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This is a common entrypoint of emitting records to tiered store. These records will be emitted to
 * the {@link BufferAccumulator} to accumulate and transform into finished buffers.
 */
public class TieredStoreProducerImpl implements TieredStoreProducer {
    private final boolean isBroadcastOnly;

    private final int numConsumers;

    private final BufferAccumulator bufferAccumulator;

    public TieredStoreProducerImpl(
            int numConsumers, boolean isBroadcastOnly, BufferAccumulator bufferAccumulator) {
        this.isBroadcastOnly = isBroadcastOnly;
        this.numConsumers = numConsumers;
        this.bufferAccumulator = bufferAccumulator;
    }

    @Override
    public void emit(
            ByteBuffer record, int consumerId, Buffer.DataType dataType, boolean isBroadcast)
            throws IOException {

        if (isBroadcast && !isBroadcastOnly) {
            for (int i = 0; i < numConsumers; ++i) {
                bufferAccumulator.receive(record.duplicate(), i, dataType);
            }
        } else {
            bufferAccumulator.receive(record, consumerId, dataType);
        }
    }

    @Override
    public void close() {
        bufferAccumulator.close();
    }

    @Override
    public void release() {
        bufferAccumulator.release();
    }
}
