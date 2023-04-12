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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.remote.RemoteTieredStorageFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.UpstreamTieredStorageFactory;

/**
 * This is a shuffle environment for tiered store. When using tiered store to shuffle data, the
 * tiered storage factories and releasers should be created from the {@link
 * TieredStoreShuffleEnvironment}.
 */
public class TieredStoreShuffleEnvironment {

    private enum TierType {
        IN_MEM,
        IN_DISK,
        IN_REMOTE,
    }

    public UpstreamTieredStorageFactory createUpstreamTieredStorageFactory(
            int[] tierIndexes,
            ResultPartitionID resultPartitionID,
            int numSubpartitions,
            float minReservedDiskSpaceFraction,
            String dataFileBasePath,
            boolean isBroadcast) {
        UpstreamTieredStorageFactory tierStorageFactory =
                new UpstreamTieredStorageFactory(
                        tierIndexes,
                        resultPartitionID,
                        numSubpartitions,
                        minReservedDiskSpaceFraction,
                        dataFileBasePath,
                        isBroadcast);
        tierStorageFactory.setup();
        return tierStorageFactory;
    }

    public RemoteTieredStorageFactory createRemoteTieredStorageFactory(int[] tierIndexes) {
        RemoteTieredStorageFactory tierStorageFactory = new RemoteTieredStorageFactory(tierIndexes);
        tierStorageFactory.setup();
        return tierStorageFactory;
    }
}
