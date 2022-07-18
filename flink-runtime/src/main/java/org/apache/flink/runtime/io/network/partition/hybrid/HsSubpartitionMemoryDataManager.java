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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.ConsumeStatus;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.SpillStatus;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for managing the data in a single subpartition. One {@link
 * HsMemoryDataManager} will hold multiple {@link HsSubpartitionMemoryDataManager}.
 */
public class HsSubpartitionMemoryDataManager {
    private final int targetChannel;

    private final int bufferSize;

    /**
     * Provider to request {@link BufferBuilder}.
     *
     * <p>Attention: Do not call this method within the subpartition lock, otherwise deadlock may
     * occur, as this callback maybe acquire other subpartition's lock inside.
     */
    private final SupplierWithException<BufferBuilder, InterruptedException> bufferProvider;

    /**
     * Callback for buffer finished.
     *
     * <p>Attention: Do not call this consumer within the read lock and subpartition lock, otherwise
     * deadlock may occur, as this callback maybe acquire write and other subpartition's lock
     * inside.
     */
    private final Runnable onBufferFinishedCallBack;

    /**
     * Callback for buffer consumed.
     *
     * <p>Attention: Do not call this consumer within the read lock and subpartition lock, otherwise
     * deadlock may occur, as this callback maybe acquire write and other subpartition's lock
     * inside.
     */
    private final Consumer<BufferIndexAndChannel> onBufferConsumedCallBack;

    /**
     * Callback to notify data index that buffer is readable.
     *
     * <p>This call back is thread safe.
     */
    private final Consumer<Integer> markBufferReadableCallBack;

    private final Lock resultPartitionReadLock;

    // this field only accessed by task main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // this field only accessed by task main thread.
    private int finishedBufferIndex;

    /**
     * Lock for subpartition's own field. All buffers status belong to this subpartition guarded by
     * this lock.
     */
    private final Object subpartitionLock = new Object();

    @GuardedBy("subpartitionLock")
    private final Deque<HsBufferContext> allBuffers = new LinkedList<>();

    @GuardedBy("subpartitionLock")
    private final Deque<HsBufferContext> unConsumedBuffers = new LinkedList<>();

    @GuardedBy("subpartitionLock")
    private final Map<Integer, HsBufferContext> bufferIndexToContexts;

    HsSubpartitionMemoryDataManager(
            int targetChannel,
            int bufferSize,
            Lock resultPartitionReadLock,
            SupplierWithException<BufferBuilder, InterruptedException> bufferProvider,
            Runnable onBufferFinishedCallBack,
            Consumer<BufferIndexAndChannel> onBufferConsumedCallBack,
            Consumer<Integer> markBufferReadableCallBack) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.resultPartitionReadLock = resultPartitionReadLock;
        this.bufferProvider = bufferProvider;
        this.onBufferFinishedCallBack = onBufferFinishedCallBack;
        this.onBufferConsumedCallBack = onBufferConsumedCallBack;
        this.markBufferReadableCallBack = markBufferReadableCallBack;
        this.bufferIndexToContexts = new HashMap<>();
    }

    // ------------------------------------------------------------------------
    //  Called by Consumer
    // ------------------------------------------------------------------------

    /**
     * Check whether the head of {@link #unConsumedBuffers} is the expected buffer. If so, return
     * the expected buffer's data type.
     *
     * @param expectedBufferIndex buffer index of target buffer.
     * @return If the head of {@link #unConsumedBuffers} is target, return the buffer's data type.
     *     Otherwise, return null.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public Buffer.DataType checkAndPeekFirstBufferDataType(int expectedBufferIndex) {
        return callWithLock(
                () -> {
                    pollReleasedBufferFromHead(unConsumedBuffers);
                    if (unConsumedBuffers.isEmpty()
                            || unConsumedBuffers
                                            .peekFirst()
                                            .getBufferIndexAndChannel()
                                            .getBufferIndex()
                                    != expectedBufferIndex) {
                        return Buffer.DataType.NONE;
                    }
                    HsBufferContext bufferContext = checkNotNull(unConsumedBuffers.peekFirst());
                    return bufferContext.getBuffer().getDataType();
                });
    }

    /**
     * Check whether the head of {@link #unConsumedBuffers} is the expected buffer. If so, return
     * the expected buffer and next data type.
     *
     * @param expectedBufferIndex buffer index of target buffer.
     * @return If the head of {@link #unConsumedBuffers} is target, return the buffer and next data
     *     type. Otherwise, return null.
     */
    @Nullable
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public HsMemoryDataManager.BufferAndNextDataType checkAndPollBuffer(int expectedBufferIndex) {
        Tuple2<HsBufferContext, Buffer.DataType> bufferAndNextDataType =
                callWithLock(
                        () -> {
                            pollReleasedBufferFromHead(unConsumedBuffers);
                            if (unConsumedBuffers.isEmpty()
                                    || unConsumedBuffers
                                                    .peekFirst()
                                                    .getBufferIndexAndChannel()
                                                    .getBufferIndex()
                                            != expectedBufferIndex) {
                                return null;
                            }

                            HsBufferContext bufferContext =
                                    checkNotNull(unConsumedBuffers.pollFirst());

                            bufferContext.onBufferConsumed();
                            pollReleasedBufferFromHead(unConsumedBuffers);
                            HsBufferContext next = unConsumedBuffers.peekFirst();
                            Buffer.DataType nextDataType =
                                    next == null
                                            ? Buffer.DataType.NONE
                                            : next.getBuffer().getDataType();
                            return Tuple2.of(bufferContext, nextDataType);
                        });

        onBufferConsumedCallBack.accept(bufferAndNextDataType.f0.getBufferIndexAndChannel());
        return new HsMemoryDataManager.BufferAndNextDataType(
                bufferAndNextDataType.f0.getBuffer(), bufferAndNextDataType.f1);
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    /**
     * Append record to {@link HsSubpartitionMemoryDataManager}.
     *
     * @param record to be managed by this class.
     * @param dataType the type of this record. In other words, is it data or event.
     */
    public void append(ByteBuffer record, Buffer.DataType dataType) throws InterruptedException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType);
        } else {
            writeRecord(record, dataType);
        }
    }

    /**
     * Get buffers in {@link #allBuffers} that satisfy expected {@link SpillStatus} and {@link
     * ConsumeStatus}.
     *
     * @param spillStatus the status of spilling expected.
     * @param consumeStatus the status of consuming expected.
     * @return buffers satisfy expected status in order.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public Deque<BufferIndexAndChannel> getBuffersSatisfyStatus(
            SpillStatus spillStatus, ConsumeStatus consumeStatus) {
        return callWithLock(
                () -> {
                    // TODO return iterator to avoid completely traversing the queue for each call.
                    Deque<BufferIndexAndChannel> targetBuffers = new ArrayDeque<>();
                    // traverse buffers in order.
                    allBuffers.forEach(
                            (bufferContext -> {
                                if (isBufferSatisfyStatus(
                                        bufferContext, spillStatus, consumeStatus)) {
                                    targetBuffers.add(bufferContext.getBufferIndexAndChannel());
                                }
                            }));
                    return targetBuffers;
                });
    }

    /**
     * Spill this subpartition's buffers in a decision.
     *
     * @param toSpill All buffers that need to be spilled belong to this subpartition in a decision.
     * @param spillDoneFuture completed when spill is finished.
     * @return {@link BufferWithIdentity}s about these spill buffers.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public List<BufferWithIdentity> spillSubpartitionBuffers(
            List<BufferIndexAndChannel> toSpill, CompletableFuture<Void> spillDoneFuture) {
        return callWithLock(
                () ->
                        toSpill.stream()
                                .map(
                                        indexAndChannel -> {
                                            int bufferIndex = indexAndChannel.getBufferIndex();
                                            HsBufferContext bufferContext =
                                                    onBufferSpill(bufferIndex, spillDoneFuture);
                                            return new BufferWithIdentity(
                                                    bufferContext.getBuffer(),
                                                    bufferIndex,
                                                    targetChannel);
                                        })
                                .collect(Collectors.toList()));
    }

    /**
     * Release this subpartition's buffers in a decision.
     *
     * @param toRelease All buffers that need to be released belong to this subpartition in a
     *     decision.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: runWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public void releaseSubpartitionBuffers(List<BufferIndexAndChannel> toRelease) {
        runWithLock(
                () ->
                        toRelease.forEach(
                                (indexAndChannel) -> {
                                    int bufferIndex = indexAndChannel.getBufferIndex();
                                    HsBufferContext bufferContext =
                                            checkNotNull(bufferIndexToContexts.get(bufferIndex));
                                    checkAndMarkBufferReadable(bufferContext);
                                    onBufferReleased(bufferIndex);
                                }));
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(ByteBuffer event, Buffer.DataType dataType) {
        checkArgument(dataType.isEvent());

        // each Event must take an exclusive buffer
        finishCurrentWritingBufferIfNotEmpty();

        // store Events in adhoc heap segments, for network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        Buffer buffer =
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size());

        HsBufferContext bufferContext =
                new HsBufferContext(buffer, finishedBufferIndex, targetChannel);
        addFinishedBuffer(bufferContext);
        onBufferFinishedCallBack.run();
    }

    private void writeRecord(ByteBuffer record, Buffer.DataType dataType)
            throws InterruptedException {
        checkArgument(!dataType.isEvent());

        ensureCapacityForRecord(record);

        writeRecord(record);
    }

    private void ensureCapacityForRecord(ByteBuffer record) throws InterruptedException {
        final int numRecordBytes = record.remaining();
        int availableBytes =
                Optional.ofNullable(unfinishedBuffers.peek())
                        .map(
                                currentWritingBuffer ->
                                        currentWritingBuffer.getWritableBytes()
                                                + bufferSize * (unfinishedBuffers.size() - 1))
                        .orElse(0);

        while (availableBytes < numRecordBytes) {
            // request unfinished buffer.
            BufferBuilder bufferBuilder = bufferProvider.get();
            unfinishedBuffers.add(bufferBuilder);
            availableBytes += bufferSize;
        }
    }

    private void writeRecord(ByteBuffer record) {
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer =
                    checkNotNull(
                            unfinishedBuffers.peek(), "Expect enough capacity for the record.");
            currentWritingBuffer.append(record);

            if (currentWritingBuffer.isFull()) {
                finishCurrentWritingBuffer();
            }
        }
    }

    private void finishCurrentWritingBufferIfNotEmpty() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer();
    }

    private void finishCurrentWritingBuffer() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.poll();

        if (currentWritingBuffer == null) {
            return;
        }

        currentWritingBuffer.finish();
        Buffer buffer = currentWritingBuffer.createBufferConsumerFromBeginning().build();
        currentWritingBuffer.close();
        // create buffer consumer will add reference count of internal buffer, decrease it to
        // keep only one reference count.
        buffer.recycleBuffer();

        HsBufferContext bufferContext =
                new HsBufferContext(buffer, finishedBufferIndex, targetChannel);
        addFinishedBuffer(bufferContext);
        onBufferFinishedCallBack.run();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    private void addFinishedBuffer(HsBufferContext bufferContext) {
        finishedBufferIndex++;
        boolean needNotify =
                callWithLock(
                        () -> {
                            allBuffers.add(bufferContext);
                            unConsumedBuffers.add(bufferContext);
                            bufferIndexToContexts.put(
                                    bufferContext.getBufferIndexAndChannel().getBufferIndex(),
                                    bufferContext);
                            return unConsumedBuffers.isEmpty();
                        });
        if (needNotify) {
            // TODO notify data available.
        }
    }

    /**
     * Poll all released buffer from head of queue until buffer queue is empty or meet un-released
     * buffer.
     */
    @GuardedBy("subpartitionLock")
    private void pollReleasedBufferFromHead(Deque<HsBufferContext> bufferQueue) {
        while (!bufferQueue.isEmpty() && bufferQueue.peekFirst().isReleased()) {
            bufferQueue.pollFirst();
        }
    }

    @GuardedBy("subpartitionLock")
    private void onBufferReleased(int bufferIndex) {
        HsBufferContext bufferContext = checkNotNull(bufferIndexToContexts.remove(bufferIndex));
        bufferContext.onBufferReleased();
        // remove released buffers from head lazy.
        pollReleasedBufferFromHead(allBuffers);
    }

    @GuardedBy("subpartitionLock")
    private HsBufferContext onBufferSpill(int bufferIndex, CompletableFuture<Void> spillFuture) {
        HsBufferContext bufferContext = checkNotNull(bufferIndexToContexts.get(bufferIndex));
        bufferContext.onBufferSpill(spillFuture);
        return bufferContext;
    }

    @GuardedBy("subpartitionLock")
    private void checkAndMarkBufferReadable(HsBufferContext bufferContext) {
        // only spill and not consumed buffer needs to be marked as readable.
        if (isBufferSatisfyStatus(bufferContext, SpillStatus.SPILL, ConsumeStatus.NOT_CONSUMED)) {
            checkNotNull(
                            bufferContext.getSpilledFuture(),
                            "Buffer in spill status should already set spilled future.")
                    .thenRun(
                            () ->
                                    markBufferReadableCallBack.accept(
                                            bufferContext
                                                    .getBufferIndexAndChannel()
                                                    .getBufferIndex()));
        }
    }

    @GuardedBy("subpartitionLock")
    private boolean isBufferSatisfyStatus(
            HsBufferContext bufferContext, SpillStatus spillStatus, ConsumeStatus consumeStatus) {
        // released buffer is not needed.
        if (bufferContext.isReleased()) {
            return false;
        }
        boolean isNeeded = true;
        switch (spillStatus) {
            case NOT_SPILL:
                isNeeded = !bufferContext.isSpill();
                break;
            case SPILL:
                isNeeded = bufferContext.isSpill();
                break;
        }
        switch (consumeStatus) {
            case NOT_CONSUMED:
                isNeeded &= !bufferContext.isConsumed();
                break;
            case CONSUMED:
                isNeeded &= bufferContext.isConsumed();
                break;
        }
        return isNeeded;
    }

    private <E extends Exception> void runWithLock(ThrowingRunnable<E> runnable) throws E {
        try {
            resultPartitionReadLock.lock();
            synchronized (subpartitionLock) {
                runnable.run();
            }
        } finally {
            resultPartitionReadLock.unlock();
        }
    }

    private <R, E extends Exception> R callWithLock(SupplierWithException<R, E> callable) throws E {
        try {
            resultPartitionReadLock.lock();
            synchronized (subpartitionLock) {
                return callable.get();
            }
        } finally {
            resultPartitionReadLock.unlock();
        }
    }
}
