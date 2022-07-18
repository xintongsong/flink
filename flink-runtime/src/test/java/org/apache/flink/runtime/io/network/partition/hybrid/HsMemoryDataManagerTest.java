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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.HsMemoryDataManager.BufferAndNextDataType;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategy.Decision;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsMemoryDataManager}. */
class HsMemoryDataManagerTest {
    private static final int NUM_BUFFERS = 10;

    private static final int MEMORY_SEGMENT_SIZE = Integer.BYTES;

    private static final int NUM_SUBPARTITIONS = 3;

    private BufferPool bufferPool;

    private FileChannel dataFileChannel;

    @BeforeEach
    void before(@TempDir Path tempDir) throws Exception {
        Path dataPath = Files.createFile(tempDir.resolve(".data"));
        dataFileChannel = FileChannel.open(dataPath, StandardOpenOption.WRITE);
        NetworkBufferPool networkBufferPool =
                new NetworkBufferPool(NUM_BUFFERS, MEMORY_SEGMENT_SIZE);
        bufferPool = networkBufferPool.createBufferPool(10, 10);
    }

    @Test
    void testAppendDataBufferFinished() throws Exception {
        AtomicInteger finishedBuffers = new AtomicInteger(0);
        HsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnBufferFinishedFunction(
                                (numTotalUnSpillBuffers) -> {
                                    finishedBuffers.incrementAndGet();
                                    return Optional.of(Decision.NO_ACTION);
                                })
                        .build();
        HsMemoryDataManager memoryDataManager =
                createMemoryDataManager(spillingStrategy, Integer.BYTES * 3);
        memoryDataManager.append(createRecord(0), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(1), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(2), 0, Buffer.DataType.DATA_BUFFER);
        assertThat(finishedBuffers).hasValue(1);
        memoryDataManager.append(createRecord(3), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(4), 0, Buffer.DataType.DATA_BUFFER);
        assertThat(finishedBuffers).hasValue(1);
        memoryDataManager.append(createRecord(5), 0, Buffer.DataType.DATA_BUFFER);
        assertThat(finishedBuffers).hasValue(2);
    }

    @Test
    void testAppendEventFinishCurrentBuffer() throws Exception {
        AtomicInteger finishedBuffers = new AtomicInteger(0);
        HsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnBufferFinishedFunction(
                                (numTotalUnSpillBuffers) -> {
                                    finishedBuffers.incrementAndGet();
                                    return Optional.of(Decision.NO_ACTION);
                                })
                        .build();
        HsMemoryDataManager memoryDataManager =
                createMemoryDataManager(spillingStrategy, Integer.BYTES * 3);
        memoryDataManager.append(createRecord(0), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(1), 0, Buffer.DataType.DATA_BUFFER);
        assertThat(finishedBuffers).hasValue(0);
        memoryDataManager.append(createRecord(2), 0, Buffer.DataType.EVENT_BUFFER);
        assertThat(finishedBuffers).hasValue(2);
    }

    @Test
    void testAppendDataRequestBuffer() throws Exception {
        HsSpillingStrategy spillingStrategy = TestingSpillingStrategy.builder().build();
        HsMemoryDataManager memoryDataManager = createMemoryDataManager(spillingStrategy);
        memoryDataManager.append(createRecord(0), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(1), 1, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(2), 2, Buffer.DataType.DATA_BUFFER);
        assertThat(memoryDataManager.getNumTotalUnSpillBuffers()).isEqualTo(3);
        assertThat(memoryDataManager.getNumTotalRequestedBuffers()).isEqualTo(3);
    }

    @Test
    void testAppendEventNotRequestBuffer() throws Exception {
        HsSpillingStrategy spillingStrategy = TestingSpillingStrategy.builder().build();
        HsMemoryDataManager memoryDataManager = createMemoryDataManager(spillingStrategy);
        memoryDataManager.append(createRecord(0), 0, Buffer.DataType.EVENT_BUFFER);
        memoryDataManager.append(createRecord(1), 1, Buffer.DataType.EVENT_BUFFER);
        memoryDataManager.append(createRecord(2), 2, Buffer.DataType.EVENT_BUFFER);
        assertThat(memoryDataManager.getNumTotalUnSpillBuffers()).isEqualTo(3);
        assertThat(memoryDataManager.getNumTotalRequestedBuffers()).isZero();
    }

    // --------------------------
    //     For DownStream
    // --------------------------
    @Test
    void testPollBufferAndRelease() throws Exception {
        HsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnBufferConsumedFunction(
                                (consumedBuffer) ->
                                        Optional.of(
                                                Decision.builder()
                                                        .addBufferToRelease(consumedBuffer)
                                                        .build()))
                        .build();

        HsMemoryDataManager memoryDataManager = createMemoryDataManager(spillingStrategy);
        memoryDataManager.append(createRecord(0), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(1), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(2), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(3), 0, Buffer.DataType.EVENT_BUFFER);

        List<Tuple2<Integer, Buffer.DataType>> expectedRecords = new ArrayList<>();
        expectedRecords.add(Tuple2.of(0, Buffer.DataType.DATA_BUFFER));
        expectedRecords.add(Tuple2.of(1, Buffer.DataType.DATA_BUFFER));
        expectedRecords.add(Tuple2.of(2, Buffer.DataType.DATA_BUFFER));
        expectedRecords.add(Tuple2.of(3, Buffer.DataType.EVENT_BUFFER));

        /*HsMemoryDataManager.HsSubpartitionDataBuffer dataBuffer =
                memoryDataManager.getSubpartitionMemoryDataManager(0);
        verifyPollRecord(
                expectedRecords,
                dataBuffer.pollBuffer(0),
                dataBuffer.pollBuffer(1),
                dataBuffer.pollBuffer(2),
                dataBuffer.pollBuffer(3));*/
    }

    private static void verifyPollRecord(
            List<Tuple2<Integer, Buffer.DataType>> expectedRecords,
            BufferAndNextDataType... bufferAndNextDataTypes) {
        checkArgument(expectedRecords.size() == bufferAndNextDataTypes.length);
        for (int i = 0; i < bufferAndNextDataTypes.length; i++) {
            Buffer buffer = bufferAndNextDataTypes[i].getBuffer();
            int value = buffer.getNioBufferReadable().getInt();
            Buffer.DataType dataType = buffer.getDataType();
            assertThat(buffer.refCnt()).isEqualTo(1);
            assertThat(value).isEqualTo(expectedRecords.get(i).f0);
            assertThat(dataType).isEqualTo(expectedRecords.get(i).f1);
            if (i != bufferAndNextDataTypes.length - 1) {
                assertThat(bufferAndNextDataTypes[i].getNextDataType())
                        .isEqualTo(expectedRecords.get(i + 1).f1);
            } else {
                assertThat(bufferAndNextDataTypes[i].getNextDataType())
                        .isEqualTo(Buffer.DataType.NONE);
            }
        }
    }

    private HsMemoryDataManager createMemoryDataManager(HsSpillingStrategy spillStrategy) {
        return new HsMemoryDataManager(
                NUM_SUBPARTITIONS, MEMORY_SEGMENT_SIZE, bufferPool, spillStrategy, dataFileChannel);
    }

    private HsMemoryDataManager createMemoryDataManager(
            HsSpillingStrategy spillStrategy, int networkBufferSize) throws Exception {
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, networkBufferSize);
        bufferPool = networkBufferPool.createBufferPool(10, 10);
        return new HsMemoryDataManager(
                NUM_SUBPARTITIONS, networkBufferSize, bufferPool, spillStrategy, dataFileChannel);
    }

    private static ByteBuffer createRecord(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(value);
        byteBuffer.flip();
        return byteBuffer;
    }
}
