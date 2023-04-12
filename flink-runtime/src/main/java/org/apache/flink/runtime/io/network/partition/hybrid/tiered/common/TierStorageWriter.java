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

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * This {@link TierStorageWriter} is the writer for specific tier. Each tier may contain data of all
 * subpartitions.
 */
public interface TierStorageWriter {

    /** Starts a new segment with the given segment index. */
    void startSegment(int consumerId, int segmentId);

    /** Writes the finished {@link Buffer} to the consumer. */
    boolean write(int consumerId, Buffer finishedBuffer) throws IOException;

    /**
     * Closes the {@link TierStorageWriter} and triggers to flushe all the cached buffers(if exist).
     */
    void close();
}
