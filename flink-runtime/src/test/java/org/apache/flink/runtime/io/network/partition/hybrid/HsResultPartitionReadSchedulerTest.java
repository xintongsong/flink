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

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsResultPartitionReadScheduler}. */
@ExtendWith(TestLoggerExtension.class)
@Timeout(60)
class HsResultPartitionReadSchedulerTest {
    private static final int BUFFER_SIZE = 1024;

    private static final int NUM_THREADS = 4;

    private static final int NUM_SUBPARTITIONS = 10;

    private final byte[] dataBytes = new byte[BUFFER_SIZE];

    private BatchShuffleReadBufferPool bufferPool;

    private ExecutorService executor;

    private FileChannel dataFileChannel;

    private HsResultPartitionReadScheduler readScheduler;

    private TestingSubpartitionViewInternalOperation subpartitionViewOperation;

    @BeforeEach
    void before(@TempDir Path tempDir) throws IOException {
        Random random = new Random();
        random.nextBytes(dataBytes);
        bufferPool = new BatchShuffleReadBufferPool(BUFFER_SIZE, BUFFER_SIZE);
        executor = Executors.newFixedThreadPool(NUM_THREADS);
        Path dataFilePath = Files.createFile(tempDir.resolve(".data"));
        dataFileChannel = openFileChannel(dataFilePath);
        readScheduler =
                new HsResultPartitionReadScheduler(
                        bufferPool,
                        executor,
                        new TestingDataIndex(),
                        dataFilePath,
                        HybridShuffleConfiguration.createConfiguration(
                                bufferPool.getNumBuffersPerRequest(), NUM_SUBPARTITIONS));
        subpartitionViewOperation = new TestingSubpartitionViewInternalOperation();
    }

    @AfterEach
    void after() throws Exception {
        bufferPool.destroy();
        executor.shutdown();
        if (dataFileChannel != null) {
            dataFileChannel.close();
        }
    }

    @Test
    void testSubpartitionReaderFail() throws Exception {
        HsSubpartitionFileReader fileReader =
                readScheduler.registerNewSubpartition(0, subpartitionViewOperation);

        assertThat(readScheduler.isRunning()).isTrue();

        fileReader.fail(new RuntimeException("expected exception."));
        // if subpartition reader is failed, read scheduler should stop as it only have one reader
        // now.
        waitUntilReadFinish(readScheduler);
    }

    @Test
    void testOnReadBufferRequestError() throws Exception {
        HsSubpartitionFileReader fileReader =
                readScheduler.registerNewSubpartition(0, subpartitionViewOperation);

        bufferPool.destroy();
        waitUntilReadFinish(readScheduler);

        assertThat(fileReader.getLoadedBuffers()).hasSize(1);
        assertThat(fileReader.getLoadedBuffers().poll())
                .isNotNull()
                .satisfies(
                        bufferIndexOrError ->
                                assertThat(bufferIndexOrError.getThrowable()).isPresent());
        assertAllResourcesReleased(readScheduler);
    }

    @Test
    void testReadSchedulerRelease() throws Exception {
        HsSubpartitionFileReader fileReader =
                readScheduler.registerNewSubpartition(0, subpartitionViewOperation);
        bufferPool.initialize();

        readScheduler.release();

        waitUntilReadFinish(readScheduler);
        assertThat(fileReader.getLoadedBuffers()).hasSize(1);
        assertThat(fileReader.getLoadedBuffers().poll())
                .isNotNull()
                .satisfies(
                        bufferIndexOrError ->
                                assertThat(bufferIndexOrError.getThrowable()).isPresent());
        assertAllResourcesReleased(readScheduler);
    }

    private void assertAllResourcesReleased(HsResultPartitionReadScheduler readScheduler) {
        assertThat(readScheduler.getDataFileChannel()).isNull();
        assertThat(readScheduler.isRunning()).isFalse();
        assertThat(readScheduler.getNumPendingReaders()).isZero();

        if (!bufferPool.isDestroyed()) {
            assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());
        }
    }

    private void waitUntilReadFinish(HsResultPartitionReadScheduler readScheduler)
            throws Exception {
        while (readScheduler.isRunning()) {
            Thread.sleep(100);
        }
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    /** Mock {@link HsFileDataIndex} for test, always return an empty {@link ReadableRegion}. */
    private static class TestingDataIndex implements HsFileDataIndex {
        @Override
        public Optional<ReadableRegion> getReadableRegion(int subpartitionId, int bufferIndex) {
            // always return empty readableRegion to prevent real read from file.
            return Optional.empty();
        }

        @Override
        public void addBuffers(List<SpilledBuffer> spilledBuffers) {
            // do nothing.
        }

        @Override
        public void markBufferReadable(int subpartitionId, int bufferIndex) {
            // do nothing.
        }
    }
}
