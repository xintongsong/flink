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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndex.SpilledBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategy.Decision;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/** This class is responsible for managing data in memory. */
public class HsMemoryDataManager implements HsSpillingInfoProvider {

    private final int numSubpartitions;

    private final HsSubpartitionMemoryDataManager[] subpartitionMemoryDataManagers;

    private final HsMemoryDataSpiller spiller;

    private final HsSpillingStrategy spillStrategy;

    private final HsFileDataIndex dataIndex;

    private final BufferPool bufferPool;

    /**
     * This lock is used for lock the entire resultPartition. As each subpartition has its own lock,
     * in order to avoid deadlock, the lock must be acquired before subpartition lock.
     */
    private final ReadWriteLock lock;

    private final AtomicInteger numRequestedBuffers;

    private final AtomicInteger numUnSpillBuffers;

    public HsMemoryDataManager(
            int numSubpartitions,
            int bufferSize,
            BufferPool bufferPool,
            HsSpillingStrategy spillStrategy,
            FileChannel dataFileChannel) {
        this.numSubpartitions = numSubpartitions;
        this.bufferPool = bufferPool;
        this.spiller = new HsMemoryDataSpiller(dataFileChannel);
        this.spillStrategy = spillStrategy;
        this.dataIndex = new HsFileDataIndexImpl(numSubpartitions);
        this.subpartitionMemoryDataManagers = new HsSubpartitionMemoryDataManager[numSubpartitions];
        this.lock = new ReentrantReadWriteLock(true);
        this.numRequestedBuffers = new AtomicInteger(0);
        this.numUnSpillBuffers = new AtomicInteger(0);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionMemoryDataManagers[subpartitionId] =
                    new HsSubpartitionMemoryDataManager(
                            subpartitionId,
                            bufferSize,
                            lock.readLock(),
                            this::requestUnfinishedBuffer,
                            this::onBufferFinished,
                            this::onBufferConsumed,
                            markSubpartitionBufferReadableCallback(subpartitionId));
        }
    }

    // ------------------------------------
    //          For ResultPartition
    // ------------------------------------

    /**
     * Append record to {@link HsMemoryDataManager}, It will be managed by {@link
     * HsSubpartitionMemoryDataManager} witch it belongs to.
     *
     * @param record to be managed by this class.
     * @param targetChannel target subpartition of this record.
     * @param dataType the type of this record. In other words, is it data or event.
     */
    public void append(ByteBuffer record, int targetChannel, Buffer.DataType dataType)
            throws IOException {
        try {
            getSubpartitionMemoryDataManager(targetChannel).append(record, dataType);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    // ------------------------------------
    //        For Spilling Strategy
    // ------------------------------------

    @Override
    public int getPoolSize() {
        return bufferPool.getNumBuffers();
    }

    @Override
    public int getNumSubpartitions() {
        return numSubpartitions;
    }

    @Override
    public int getNumTotalRequestedBuffers() {
        return numRequestedBuffers.get();
    }

    @Override
    public int getNumTotalUnSpillBuffers() {
        return numUnSpillBuffers.get();
    }

    // Write lock should be acquired before invoke this method.
    @Override
    public Deque<BufferIndexAndChannel> getBuffersInOrder(
            int subpartitionId, SpillStatus spillStatus, ConsumeStatus consumeStatus) {
        HsSubpartitionMemoryDataManager targetSubpartitionDataBuffer =
                getSubpartitionMemoryDataManager(subpartitionId);
        return targetSubpartitionDataBuffer.getBuffersSatisfyStatus(spillStatus, consumeStatus);
    }

    // Write lock should be acquired before invoke this method.
    @Override
    public List<Integer> getNextBufferIndexToConsume() {
        // TODO implements this logical when subpartition view is implemented.
        return Collections.emptyList();
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    private Consumer<Integer> markSubpartitionBufferReadableCallback(int subpartitionId) {
        return (bufferIndex) -> dataIndex.markBufferReadable(subpartitionId, bufferIndex);
    }

    /** Buffer provider for {@link HsSubpartitionMemoryDataManager}. */
    private BufferBuilder requestUnfinishedBuffer() throws InterruptedException {
        MemorySegment segment = bufferPool.requestMemorySegmentBlocking();
        Optional<Decision> decisionOpt =
                spillStrategy.onMemoryUsageChanged(
                        numRequestedBuffers.incrementAndGet(), getPoolSize());

        handleDecision(decisionOpt);
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    /** Callback for {@link HsSubpartitionMemoryDataManager}, when subpartition buffer consumed. */
    private void onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        Optional<Decision> decision = spillStrategy.onBufferConsumed(consumedBuffer);
        handleDecision(decision);
    }

    /** Callback for {@link HsSubpartitionMemoryDataManager}, when subpartition buffer finished. */
    private void onBufferFinished() {
        Optional<Decision> decision =
                spillStrategy.onBufferFinished(numUnSpillBuffers.incrementAndGet());
        handleDecision(decision);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    // Note that: Do not call this method within the subpartition lock, otherwise deadlock may
    // occur, as spillBuffers and releaseBuffers maybe acquire other subpartition's lock, and this
    // method must be outside the read lock, because write lock may be accessed inside, which may
    // block forever.
    private void handleDecision(
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
                    Optional<Decision> decisionOpt) {
        Decision decision =
                decisionOpt.orElseGet(
                        () ->
                                callWithWriteLock(
                                        () -> spillStrategy.decideActionWithGlobalInfo(this)));

        if (!decision.getBufferToSpill().isEmpty()) {
            spillBuffers(decision.getBufferToSpill());
        }
        if (!decision.getBufferToRelease().isEmpty()) {
            releaseBuffers(decision.getBufferToRelease());
        }
    }

    /**
     * Spill buffers for each subpartition in a decision.
     *
     * <p>Note that: The method should not be locked, it is the responsibility of each subpartition
     * to maintain thread safety itself.
     *
     * @param toSpill All buffers that need to be spilled in a decision.
     */
    private void spillBuffers(Map<Integer, List<BufferIndexAndChannel>> toSpill) {
        CompletableFuture<List<SpilledBuffer>> spilledBuffersFuture = new CompletableFuture<>();
        CompletableFuture<Void> spillDoneFuture =
                FutureUtils.runAfterwards(spilledBuffersFuture, () -> {});
        List<BufferWithIdentity> bufferWithIdentities = new ArrayList<>();
        toSpill.forEach(
                (subpartitionId, bufferIndexAndChannels) -> {
                    HsSubpartitionMemoryDataManager subpartitionDataBuffer =
                            getSubpartitionMemoryDataManager(subpartitionId);
                    bufferWithIdentities.addAll(
                            subpartitionDataBuffer.spillSubpartitionBuffers(
                                    bufferIndexAndChannels, spillDoneFuture));
                    // decrease numUnSpillBuffers as this subpartition's buffer is spill.
                    numUnSpillBuffers.getAndAdd(-bufferIndexAndChannels.size());
                });

        spiller.spillAsync(bufferWithIdentities, spilledBuffersFuture);

        // update data index when spilling operation finished.
        spilledBuffersFuture.thenAccept(
                // in spilling thread, this method is thread safe.
                dataIndex::addBuffers);
    }

    /**
     * Release buffers for each subpartition in a decision.
     *
     * <p>Note that: The method should not be locked, it is the responsibility of each subpartition
     * to maintain thread safety itself.
     *
     * @param toRelease All buffers that need to be released in a decision.
     */
    private void releaseBuffers(Map<Integer, List<BufferIndexAndChannel>> toRelease) {
        toRelease.forEach(
                (subpartitionId, subpartitionBuffers) ->
                        getSubpartitionMemoryDataManager(subpartitionId)
                                .releaseSubpartitionBuffers(subpartitionBuffers));
    }

    private HsSubpartitionMemoryDataManager getSubpartitionMemoryDataManager(int targetChannel) {
        return subpartitionMemoryDataManagers[targetChannel];
    }

    private void recycleBuffer(MemorySegment buffer) {
        numRequestedBuffers.decrementAndGet();
        bufferPool.recycle(buffer);
    }

    public <T, R extends Exception> T callWithWriteLock(SupplierWithException<T, R> callable)
            throws R {
        try {
            lock.writeLock().lock();
            return callable.get();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Integrate the buffer and dataType of next buffer. */
    public static class BufferAndNextDataType {
        private final Buffer buffer;

        private final Buffer.DataType nextDataType;

        public BufferAndNextDataType(Buffer buffer, Buffer.DataType nextDataType) {
            this.buffer = buffer;
            this.nextDataType = nextDataType;
        }

        public Buffer getBuffer() {
            return buffer;
        }

        public Buffer.DataType getNextDataType() {
            return nextDataType;
        }
    }
}
