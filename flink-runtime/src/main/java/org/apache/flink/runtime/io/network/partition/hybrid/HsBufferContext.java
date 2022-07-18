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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class maintains the buffer's reference count and its status for hybrid shuffle mode.
 *
 * <p>Each buffer has three status: {@link #isReleased}, {@link #isSpill}, {@link #isConsumed}.If
 * buffer is released it can no longer set isSpill and isConsumed status to true.
 *
 * <ul>
 *   <li>{@link #isReleased} will be true when buffer is released from memory.
 *   <li>{@link #isSpill} will be true when buffer is decided to spill.
 *   <li>{@link #isConsumed} will be true when buffer is consumed by downStream.
 * </ul>
 *
 * <p>Buffer will increase or decrease the reference count when the status changes in a dual way.
 *
 * <ul>
 *   <li>spill: +1, spilled: -1
 *   <li>consumed: +1, consuming completed(netty thread send buffer to {@link RemoteInputChannel} or
 *       handled by {@link LocalInputChannel}): -1
 *   <li>created: +1, released from {@link HsMemoryDataManager}: -1
 * </ul>
 *
 * <p>Note that: This class itself is not thread-safe.
 */
@NotThreadSafe
public class HsBufferContext {
    private final Buffer buffer;

    private final BufferIndexAndChannel bufferIndexAndChannel;

    // --------------------------
    //      Buffer Status
    // --------------------------
    private boolean isReleased;

    private boolean isSpill;

    private boolean isConsumed;

    @Nullable private CompletableFuture<Void> spilledFuture;

    public HsBufferContext(Buffer buffer, int bufferIndex, int subpartitionId) {
        this.bufferIndexAndChannel = new BufferIndexAndChannel(bufferIndex, subpartitionId);
        this.buffer = buffer;
    }

    public void onBufferReleased() {
        checkState(!isReleased, "Release buffer repeatedly is unexpected.");
        isReleased = true;
        // decrease ref count when buffer is released from memory.
        buffer.recycleBuffer();
    }

    public void onBufferSpill(CompletableFuture<Void> spilledFuture) {
        checkState(!isReleased, "Buffer is already released.");
        checkState(
                !isSpill && this.spilledFuture == null, "Spill buffer repeatedly is unexpected.");
        isSpill = true;
        // increase ref count when buffer is decided to spill.
        buffer.retainBuffer();
        // decrease ref count when buffer spilling is finished.
        spilledFuture.thenRun(buffer::recycleBuffer);
        this.spilledFuture = spilledFuture;
    }

    public void onBufferConsumed() {
        checkState(!isReleased, "Buffer is already released.");
        checkState(!isConsumed, "Consume buffer repeatedly is unexpected.");
        isConsumed = true;
        // increase ref count when buffer is consumed, will be decreased when downstream finish
        // consuming.
        buffer.retainBuffer();
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public BufferIndexAndChannel getBufferIndexAndChannel() {
        return bufferIndexAndChannel;
    }

    @Nullable
    public CompletableFuture<Void> getSpilledFuture() {
        return spilledFuture;
    }

    public boolean isReleased() {
        return isReleased;
    }

    public boolean isSpill() {
        return isSpill;
    }

    public boolean isConsumed() {
        return isConsumed;
    }
}
