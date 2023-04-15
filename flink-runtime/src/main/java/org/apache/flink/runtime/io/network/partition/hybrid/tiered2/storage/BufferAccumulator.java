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
import org.apache.flink.runtime.io.network.partition.hybrid.tiered2.common.TieredStorageSubpartitionId;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

/** Accumulates received records into buffers. */
public interface BufferAccumulator {

    /**
     * Setup the accumulator.
     *
     * @param numSubpartitions number of subpartitions
     * @param flusher accepts the accumulated buffers. The index of the outer list corresponds to
     *     the subpartition ids, while each inner list contains accumulated buffers in order for
     *     that subpartition.
     */
    void setup(int numSubpartitions, Consumer<List<List<Buffer>>> flusher);

    /** Receive a new record. */
    void receive(ByteBuffer record, TieredStorageSubpartitionId subpartitionId);

    /**
     * Close the accumulator. This will flush all the remaining data and release all the resources.
     */
    void close();
}
