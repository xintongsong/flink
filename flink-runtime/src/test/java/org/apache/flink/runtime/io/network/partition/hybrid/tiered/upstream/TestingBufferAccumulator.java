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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.cache.BufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.upstream.cache.MemorySegmentAndConsumerId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** Test implementation for {@link BufferAccumulator} . */
public class TestingBufferAccumulator implements BufferAccumulator {
    @Override
    public void receive(ByteBuffer record, int consumerId, Buffer.DataType dataType)
            throws IOException {}

    @Override
    public void writeFinishedBuffer(List<MemorySegmentAndConsumerId> memorySegmentAndConsumerIds) {}

    @Override
    public void close() {}

    @Override
    public void release() {}
}
