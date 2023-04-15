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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.common.TieredStorageTopicId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.tier.TierFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.Collections;
import java.util.List;

public class TieredShuffleUtils {

    public static List<TierFactory> getTierFactoriesFromDescriptor(
            TieredShuffleDescriptor descriptor) {
        return Collections.emptyList();
    }

    public static TieredStorageTopicId convertId(IntermediateDataSetID intermediateDataSetID) {
        return null;
    }

    public static IntermediateDataSetID convertId(TieredStorageTopicId topicId) {
        return null;
    }

    public static TieredStoragePartitionId convertId(ResultPartitionID resultPartitionId) {
        return null;
    }

    public static ResultPartitionID convertId(TieredStoragePartitionId partitionId) {
        return null;
    }

    public static TieredStorageSubpartitionId convertId(
            ResultPartitionID resultPartitionId, int subpartitionId) {
        return null;
    }

    public static TieredStorageSubpartitionId convertId(
            TieredStoragePartitionId partitionId, int subpartitionId) {
        return null;
    }

    public static Tuple2<ResultPartitionID, Integer> convertId(
            TieredStorageSubpartitionId subpartitionId) {
        return null;
    }
}
