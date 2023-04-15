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

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.storage.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.storage.ResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.storage.TieredStorageProducerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.tier.TierProducerAgent;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class TieredShuffleEnvironment
        implements ShuffleEnvironment<TieredResultPartition, TieredInputGate> {

    private final ResourceRegistry resourceRegistry = new ResourceRegistry();

    @Override
    public List<TieredResultPartition> createResultPartitionWriters(
            ShuffleIOOwnerContext ownerContext,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors) {

        return resultPartitionDeploymentDescriptors.stream()
                .map(this::createResultPartitionWriter)
                .collect(Collectors.toList());
    }

    private TieredResultPartition createResultPartitionWriter(
            ResultPartitionDeploymentDescriptor descriptor) {
        // TODO decide accumulator from descriptor or configuration
        BufferAccumulator accumulator = null;

        List<TierProducerAgent> producerAgents =
                TieredShuffleUtils.getTierFactoriesFromDescriptor(
                                (TieredShuffleDescriptor) descriptor.getShuffleDescriptor())
                        .stream()
                        .map(TierFactory::createProducerAgent)
                        .collect(Collectors.toList());

        TieredStoragePartitionId partitionId =
                TieredShuffleUtils.convertId(
                        descriptor.getShuffleDescriptor().getResultPartitionID());
        TieredStorageProducerClient producerClient =
                new TieredStorageProducerClient(
                        partitionId,
                        descriptor.getNumberOfSubpartitions(),
                        accumulator,
                        producerAgents,
                        resourceRegistry);
        return new TieredResultPartition(partitionId, producerClient, resourceRegistry);
    }

    //
    //
    //

    @Override
    public int start() throws IOException {
        return 0;
    }

    @Override
    public ShuffleIOOwnerContext createShuffleIOOwnerContext(
            String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup parentGroup) {
        return null;
    }

    @Override
    public void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds) {}

    @Override
    public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
        return null;
    }

    @Override
    public List<TieredInputGate> createInputGates(
            ShuffleIOOwnerContext ownerContext,
            PartitionProducerStateProvider partitionProducerStateProvider,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {
        return null;
    }

    @Override
    public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo)
            throws IOException, InterruptedException {
        return false;
    }

    @Override
    public void close() throws Exception {}
}
