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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndex.SpilledBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSubpartitionFileReader.BufferIndexOrError;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HsSubpartitionFileReader}. */
@ExtendWith(TestLoggerExtension.class)
class HsSubpartitionFileReaderTest {
    private static final int bufferSize = 1024;

    private static final byte[] dataBytes = new byte[bufferSize];

    private static final int targetChannel = 0;

    private static final int MAX_BUFFERS_READ_AHEAD = 5;

    private HsFileDataIndex diskIndex;

    private TestingSubpartitionViewInternalOperation subpartitionOperation;

    private FileChannel dataFileChannel;

    private long currentFileOffset;

    @BeforeEach
    void before(@TempDir Path tempPath) throws Exception {
        Random random = new Random();
        random.nextBytes(dataBytes);
        Path dataFilePath = Files.createFile(tempPath.resolve(".data"));
        dataFileChannel = openFileChannel(dataFilePath);
        diskIndex = new HsFileDataIndexImpl(1);
        subpartitionOperation = new TestingSubpartitionViewInternalOperation();
        currentFileOffset = 0L;
    }

    @AfterEach
    void after() {
        IOUtils.closeQuietly(dataFileChannel);
    }

    /**
     * If target buffer is not the first buffer in the region, file reader will skip the buffers not
     * needed.
     */
    @Test
    void testReadSkipBuffersBeforeTarget() throws Exception {
        HsSubpartitionFileReader subpartitionFileReader = createSubpartitionFileReader();
        Deque<BufferIndexOrError> loadedBuffers = subpartitionFileReader.getLoadedBuffers();
        HsSubpartitionFileReader.BufferIndexManager bufferIndexManager =
                subpartitionFileReader.getBufferIndexManager();
        // write buffer with index: 0, 1, 2, 3, 4, 5
        writeDataToFile(0, 6);

        subpartitionOperation.advanceConsumptionProgress();
        subpartitionOperation.advanceConsumptionProgress();
        assertThat(subpartitionOperation.getConsumingOffset()).isEqualTo(1);
        // update consumptionProgress
        subpartitionFileReader.updateConsumptionProgress();
        assertThat(bufferIndexManager.getNextToLoad()).isEqualTo(2);
        // read buffer, expected buffer with index: 2
        Queue<MemorySegment> segments = createsMemorySegments(1);
        subpartitionFileReader.readBuffers(segments, FreeingBufferRecycler.INSTANCE);

        assertThat(segments).isEmpty();
        assertThat(loadedBuffers).hasSize(1);

        BufferIndexOrError bufferIndexOrError = loadedBuffers.poll();
        assertThat(bufferIndexOrError.getBuffer()).isPresent();
        assertThat(bufferIndexOrError.getThrowable()).isNotPresent();
        assertThat(bufferIndexOrError.getIndex()).isEqualTo(2);

        subpartitionOperation.advanceConsumptionProgress();
        subpartitionOperation.advanceConsumptionProgress();
        subpartitionFileReader.updateConsumptionProgress();
        assertThat(bufferIndexManager.getNextToLoad()).isEqualTo(4);
        segments = createsMemorySegments(1);
        // trigger next round read, cached region will not update, but numSkip, numReadable and
        // currentBufferIndex should be updated.
        subpartitionFileReader.readBuffers(segments, FreeingBufferRecycler.INSTANCE);
        assertThat(segments).isEmpty();
        assertThat(loadedBuffers).hasSize(1);

        bufferIndexOrError = loadedBuffers.poll();
        assertThat(bufferIndexOrError.getBuffer()).isPresent();
        assertThat(bufferIndexOrError.getThrowable()).isNotPresent();
        assertThat(bufferIndexOrError.getIndex()).isEqualTo(4);
    }

    @Test
    void testPreLoadWillNotExceedThreshold() throws Exception {
        HsSubpartitionFileReader subpartitionFileReader = createSubpartitionFileReader();
        Deque<BufferIndexOrError> loadedBuffers = subpartitionFileReader.getLoadedBuffers();
        HsSubpartitionFileReader.BufferIndexManager bufferIndexManager =
                subpartitionFileReader.getBufferIndexManager();

        // write maxBuffersReadAhead buffers form one region to file
        writeDataToFile(0, MAX_BUFFERS_READ_AHEAD + 1);

        subpartitionFileReader.updateConsumptionProgress();
        assertThat(bufferIndexManager.getNextToLoad()).isEqualTo(0);

        // allocate maxBuffersReadAhead + 1 read buffers for reading
        Queue<MemorySegment> memorySegments = createsMemorySegments(MAX_BUFFERS_READ_AHEAD + 1);
        // trigger reading
        subpartitionFileReader.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);

        // preload by fileReader will not exceed threshold
        assertThat(loadedBuffers).hasSize(MAX_BUFFERS_READ_AHEAD);
        assertThat(memorySegments).hasSize(1);
        assertThat(bufferIndexManager.getNextToLoad()).isEqualTo(-1);

        // advance consumption progress
        subpartitionOperation.advanceConsumptionProgress();
        subpartitionFileReader.updateConsumptionProgress();
        assertThat(bufferIndexManager.getNextToLoad()).isEqualTo(MAX_BUFFERS_READ_AHEAD);
    }

    @Test
    void testNotifyDataAvailable() throws Exception {
        HsSubpartitionFileReader subpartitionFileReader = createSubpartitionFileReader();
        BlockingDeque<BufferIndexOrError> loadedBuffers =
                (BlockingDeque<BufferIndexOrError>) subpartitionFileReader.getLoadedBuffers();

        writeDataToFile(0, 1);
        subpartitionFileReader.updateConsumptionProgress();
        Queue<MemorySegment> memorySegments = createsMemorySegments(1);
        subpartitionFileReader.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);
        assertThat(subpartitionOperation.getNotifyNum()).isEqualTo(1);
        assertThat(loadedBuffers).hasSize(1);

        // trigger next round reading.
        final int numBuffers = MAX_BUFFERS_READ_AHEAD;
        subpartitionOperation.advanceConsumptionProgress();
        writeDataToFile(1, numBuffers);
        subpartitionFileReader.updateConsumptionProgress();

        OneShotLatch consumerThreadLatch = new OneShotLatch();
        AtomicReference<Throwable> cause = new AtomicReference<>(null);
        new Thread(
                        () -> {
                            consumerThreadLatch.trigger();
                            for (int i = 0; i < numBuffers; i++) {
                                try {
                                    loadedBuffers.take();
                                } catch (InterruptedException e) {
                                    cause.set(e);
                                }
                            }
                        })
                .start();
        consumerThreadLatch.await();

        // trigger reading
        subpartitionFileReader.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);
        assertThat(cause.get()).isNull();
        assertThat(subpartitionOperation.getNotifyNum()).isLessThanOrEqualTo(2);
    }

    @Test
    void testFail() throws Exception {
        HsSubpartitionFileReader subpartitionFileReader = createSubpartitionFileReader();
        Deque<BufferIndexOrError> loadedBuffers = subpartitionFileReader.getLoadedBuffers();

        writeDataToFile(0, 2);

        subpartitionFileReader.updateConsumptionProgress();
        Queue<MemorySegment> memorySegments = createsMemorySegments(2);
        // trigger reading
        subpartitionFileReader.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);
        assertThat(memorySegments).isEmpty();
        assertThat(loadedBuffers).hasSize(2);
        assertThat(subpartitionOperation.getNotifyNum()).isEqualTo(1);

        subpartitionFileReader.fail(new RuntimeException("expected exception."));
        BufferIndexOrError error = loadedBuffers.poll();
        assertThat(loadedBuffers).isEmpty();
        assertThat(error).isNotNull();
        assertThat(error.getThrowable())
                .hasValueSatisfying(
                        throwable ->
                                assertThat(throwable)
                                        .isInstanceOf(RuntimeException.class)
                                        .hasMessage("expected exception."));

        // subpartitionReader fail should notify downstream.
        assertThat(subpartitionOperation.getNotifyNum()).isEqualTo(2);

        assertThatThrownBy(
                        () ->
                                subpartitionFileReader.readBuffers(
                                        createsMemorySegments(2), FreeingBufferRecycler.INSTANCE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("subpartition reader has already failed.");
    }

    @Test
    void testCompareTo() throws Exception {
        diskIndex = new HsFileDataIndexImpl(2);
        TestingSubpartitionViewInternalOperation viewNotifier1 =
                new TestingSubpartitionViewInternalOperation();
        TestingSubpartitionViewInternalOperation viewNotifier2 =
                new TestingSubpartitionViewInternalOperation();
        HsSubpartitionFileReader fileReader1 = createSubpartitionFileReader(0, viewNotifier1);
        HsSubpartitionFileReader fileReader2 = createSubpartitionFileReader(1, viewNotifier2);
        assertThat(fileReader1).isEqualByComparingTo(fileReader2);

        // buffers in file: (0-0, 0-1, 1-0, 0-2)
        writeDataToFile(0, 0, 2);
        writeDataToFile(1, 0, 1);
        writeDataToFile(0, 2, 1);
        // fileReader1 -> (0-0) fileReader2 -> (1-0)
        assertThat(fileReader1).isLessThan(fileReader2);

        viewNotifier1.advanceConsumptionProgress();
        fileReader1.updateConsumptionProgress();
        // fileReader1 -> (0-1) fileReader2 -> (1-0)
        assertThat(fileReader1).isLessThan(fileReader2);

        viewNotifier1.advanceConsumptionProgress();
        fileReader1.updateConsumptionProgress();
        // fileReader1 -> (0-2) fileReader2 -> (1-0)
        assertThat(fileReader1).isGreaterThan(fileReader2);
    }

    private HsSubpartitionFileReader createSubpartitionFileReader() {
        return createSubpartitionFileReader(targetChannel, subpartitionOperation);
    }

    private HsSubpartitionFileReader createSubpartitionFileReader(
            int targetChannel, HsSubpartitionViewInternalOperations subpartitionViewNotifier) {
        return new HsSubpartitionFileReader(
                targetChannel,
                dataFileChannel,
                subpartitionViewNotifier,
                diskIndex,
                MAX_BUFFERS_READ_AHEAD);
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private static Queue<MemorySegment> createsMemorySegments(int numSegments) {
        Queue<MemorySegment> segments = new ArrayDeque<>();
        for (int i = 0; i < numSegments; ++i) {
            segments.add(MemorySegmentFactory.allocateUnpooledSegment(bufferSize));
        }
        return segments;
    }

    private void writeDataToFile(int subpartitionId, int firstBufferIndex, int numBuffers)
            throws IOException {
        List<SpilledBuffer> spilledBuffers = new ArrayList<>(numBuffers);
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * numBuffers];
        int totalBytes = 0;

        for (int i = 0; i < numBuffers; i++) {
            Buffer.DataType dataType =
                    i == numBuffers - 1
                            ? Buffer.DataType.EVENT_BUFFER
                            : Buffer.DataType.DATA_BUFFER;

            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
            segment.put(0, dataBytes);
            Buffer buffer =
                    new NetworkBuffer(
                            segment, FreeingBufferRecycler.INSTANCE, dataType, bufferSize);
            setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
            spilledBuffers.add(
                    new SpilledBuffer(
                            subpartitionId, firstBufferIndex + i, currentFileOffset + totalBytes));
            totalBytes += bufferSize + BufferReaderWriterUtil.HEADER_LENGTH;
        }

        BufferReaderWriterUtil.writeBuffers(dataFileChannel, totalBytes, bufferWithHeaders);
        currentFileOffset += totalBytes;

        diskIndex.addBuffers(spilledBuffers);
        // mark all buffers status to release.
        spilledBuffers.forEach(
                spilledBuffer ->
                        diskIndex.markBufferReadable(subpartitionId, spilledBuffer.bufferIndex));
    }

    private void writeDataToFile(int firstBufferIndex, int numBuffers) throws IOException {
        writeDataToFile(targetChannel, firstBufferIndex, numBuffers);
    }

    private static void setBufferWithHeader(
            Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();
    }
}
