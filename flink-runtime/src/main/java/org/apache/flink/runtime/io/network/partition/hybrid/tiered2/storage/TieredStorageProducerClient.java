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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered2.storage;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.tier.TierProducerAgent;

import java.nio.ByteBuffer;
import java.util.List;

/** Client of the Tiered Storage used by the producer. */
public class TieredStorageProducerClient {

    // Partition information
    private final TieredStoragePartitionId partitionId;
    private final int numSubpartitions;

    // Internal components
    private final BufferAccumulator accumulator;
    private final List<TierProducerAgent> tiers;

    // Context services
    private final ResourceRegistry resourceRegistry;

    public TieredStorageProducerClient(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            BufferAccumulator accumulator,
            List<TierProducerAgent> tiers,
            ResourceRegistry resourceRegistry) {
        this.partitionId = partitionId;
        this.numSubpartitions = numSubpartitions;
        this.accumulator = accumulator;
        this.tiers = tiers;
        this.resourceRegistry = resourceRegistry;

        this.accumulator.setup(this.numSubpartitions, this::flushAccumulatedBuffers);
        this.resourceRegistry.registerResource(
                this.partitionId,
                () -> {
                    // remove local files
                    // close io scheduler
                });
    }

    public void write(ByteBuffer record, TieredStorageSubpartitionId subpartitionId) {
        accumulator.receive(record, subpartitionId);
    }

    /**
     * Close the producer client and release all resources for writing data into the storage.
     *
     * <p>Note that resources for maintaining the data will not be released, until the data is
     * removed.
     */
    public void close() {
        accumulator.close();
        tiers.forEach(TierProducerAgent::close);
    }

    private void flushAccumulatedBuffers(List<List<Buffer>> buffersBySubpartition) {
        // TODO:
        //  details omitted,
        //  should call TierProducerAgent#tryStartNewSegment and TierProducerAgent#write

        TierProducerAgent tier = tiers.get(0);
        Buffer buffer = buffersBySubpartition.get(0).get(0);
        TieredStorageSubpartitionId subpartitionId = null;
        int segmentId = 0;

        tier.tryStartNewSegment(subpartitionId, segmentId);
        tier.write(subpartitionId, buffer);
    }
}
