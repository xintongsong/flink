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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.common;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.TieredStoreResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.cache.BufferAccumulator;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The producer interface of tiered store. {@link TieredStoreResultPartition} emits the records to
 * the {@link TieredStoreProducer} firstly. The records are accumulated to finished buffers in
 * {@link BufferAccumulator} and The producer chooses a storage tier to store the accumulated
 * finished buffer.
 */
public interface TieredStoreProducer {

    /**
     * Receives the records from {@link TieredStoreResultPartition}, the records will be emitted to
     * {@link BufferAccumulator} for transforming them into finished {@link MemorySegment}.
     */
    void emit(ByteBuffer record, int consumerId, Buffer.DataType dataType, boolean isBroadcast)
            throws IOException;

    /** Closes the {@link TieredStoreProducer}. */
    void close();

    /** Releases the {@link TieredStoreProducer}, all resources should be released. */
    void release();
}
