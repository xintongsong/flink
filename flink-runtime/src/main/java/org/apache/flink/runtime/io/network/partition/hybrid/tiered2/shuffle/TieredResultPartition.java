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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered2.shuffle;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.storage.ResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.storage.TieredStorageProducerClient;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class TieredResultPartition implements ResultPartitionWriter {

    private final TieredStoragePartitionId partitionId;
    private final TieredStorageProducerClient producerClient;
    private final ResourceRegistry resourceRegistry;

    public TieredResultPartition(
            TieredStoragePartitionId partitionId,
            TieredStorageProducerClient producerClient,
            ResourceRegistry resourceRegistry) {
        this.partitionId = partitionId;
        this.producerClient = producerClient;
        this.resourceRegistry = resourceRegistry;
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        producerClient.write(record, TieredShuffleUtils.convertId(partitionId, targetSubpartition));
    }

    @Override
    public void close() throws Exception {
        producerClient.close();
    }

    @Override
    public void release(Throwable cause) {
        resourceRegistry.clearResourceFor(partitionId);
    }

    //
    //
    //

    @Override
    public void setup() throws IOException {}

    @Override
    public ResultPartitionID getPartitionId() {
        return null;
    }

    @Override
    public int getNumberOfSubpartitions() {
        return 0;
    }

    @Override
    public int getNumTargetKeyGroups() {
        return 0;
    }

    @Override
    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {}

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {}

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {}

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {}

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {}

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {}

    @Override
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        return null;
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {}

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int index, BufferAvailabilityListener availabilityListener) throws IOException {
        return null;
    }

    @Override
    public void flushAll() {}

    @Override
    public void flush(int subpartitionIndex) {}

    @Override
    public void fail(@Nullable Throwable throwable) {}

    @Override
    public void finish() throws IOException {}

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public boolean isReleased() {
        return false;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return null;
    }
}
