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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.tier.TierMasterAgent;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import java.util.concurrent.CompletableFuture;

public class TieredShuffleMaster implements ShuffleMaster<TieredShuffleDescriptor> {

    private final TierMasterAgent[] tiers;

    public TieredShuffleMaster(TierMasterAgent[] tiers) {
        this.tiers = tiers;
    }

    @Override
    public CompletableFuture<TieredShuffleDescriptor> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor) {
        return null;
    }

    @Override
    public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {}
}
