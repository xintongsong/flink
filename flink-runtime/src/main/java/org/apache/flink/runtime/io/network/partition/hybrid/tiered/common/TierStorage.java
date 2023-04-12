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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import java.io.IOException;

/**
 * The single storage tier writer for the tiered store. The storage tier writers is independent of
 * each other. Through the {@link TierStorageWriter}, we can create {@link TierStorage} to store
 * shuffle data.
 */
public interface TierStorage {

    void setup() throws IOException;

    /** Create the {@link TierStorage} of the {@link TierStorageWriter} to write shuffle data. */
    TierStorageWriter createTierStorageWriter();

    /**
     * Before start a new segment, the writer side calls the method to check whether the {@link
     * TierStorageWriter} can store the next segment.
     */
    boolean canStoreNextSegment(int subpartitionId);

    /** Returns the index of this tier. */
    int getTierIndex();

    /** Releases the {@link TierStorageWriter}, all resources should be released. */
    void release();
}
