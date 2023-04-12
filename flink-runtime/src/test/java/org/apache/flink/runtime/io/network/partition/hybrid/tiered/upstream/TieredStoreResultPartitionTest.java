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

import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TierStorage;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TieredStoreResultPartition}. */
class TieredStoreResultPartitionTest {
    private static final int bufferSize = 1024;

    private static final int totalBuffers = 1000;

    private static final int totalBytes = 32 * 1024 * 1024;

    private static final int numThreads = 4;

    private FileChannelManager fileChannelManager;

    private NetworkBufferPool globalPool;

    private BatchShuffleReadBufferPool readBufferPool;

    private ScheduledExecutorService readIOExecutor;

    private TaskIOMetricGroup taskIOMetricGroup;

    @TempDir public java.nio.file.Path tempDataPath;

    @BeforeEach
    void before() {
        fileChannelManager =
                new FileChannelManagerImpl(new String[] {tempDataPath.toString()}, "testing");
        globalPool = new NetworkBufferPool(totalBuffers, bufferSize);
        readBufferPool = new BatchShuffleReadBufferPool(totalBytes, bufferSize);
        readIOExecutor =
                new ScheduledThreadPoolExecutor(
                        numThreads,
                        new ExecutorThreadFactory("test-io-scheduler-thread"),
                        (ignored, executor) -> {
                            if (executor.isShutdown()) {
                                // ignore rejected as shutdown.
                            } else {
                                throw new RejectedExecutionException();
                            }
                        });
    }

    @AfterEach
    void after() throws Exception {
        fileChannelManager.close();
        globalPool.destroy();
        readBufferPool.destroy();
        readIOExecutor.shutdown();
    }

    @Test
    void testClose() throws Exception {
        final int numBuffers = 1;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredStoreResultPartition partition =
                createTieredStoreResultPartition(1, bufferPool, false);

        partition.close();
        assertThat(bufferPool.isDestroyed()).isTrue();
    }

    @Test
    @Timeout(30)
    void testRelease() throws Exception {
        final int numSubpartitions = 2;
        final int numBuffers = 10;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredStoreResultPartition partition =
                createTieredStoreResultPartition(numSubpartitions, bufferPool, false);

        partition.emitRecord(ByteBuffer.allocate(bufferSize * 5), 1);
        partition.close();
        assertThat(bufferPool.isDestroyed()).isTrue();

        partition.release();

        while (checkNotNull(fileChannelManager.getPaths()[0].listFiles()).length != 0) {
            Thread.sleep(10);
        }

        assertThat(totalBuffers).isEqualTo(globalPool.getNumberOfAvailableMemorySegments());
    }

    @Test
    void testCreateSubpartitionViewAfterRelease() throws Exception {
        final int numBuffers = 10;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredStoreResultPartition resultPartition =
                createTieredStoreResultPartition(2, bufferPool, false);
        resultPartition.release();
        assertThatThrownBy(
                        () ->
                                resultPartition.createSubpartitionView(
                                        0, new NoOpBufferAvailablityListener()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testEmitRecords() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(3, 3);
        try (TieredStoreResultPartition partition =
                createTieredStoreResultPartition(2, bufferPool, false)) {
            partition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            IOMetrics ioMetrics = taskIOMetricGroup.createSnapshot();
            assertThat(ioMetrics.getResultPartitionBytes()).hasSize(1);
            ResultPartitionBytes partitionBytes =
                    ioMetrics.getResultPartitionBytes().values().iterator().next();
            assertThat(partitionBytes.getSubpartitionBytes())
                    .containsExactly((long) 2 * bufferSize, bufferSize);
        }
    }

    @Test
    void testMetricsUpdateForBroadcastOnlyResultPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(3, 3);
        try (TieredStoreResultPartition partition =
                createTieredStoreResultPartition(2, bufferPool, true)) {
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            IOMetrics ioMetrics = taskIOMetricGroup.createSnapshot();
            assertThat(ioMetrics.getResultPartitionBytes()).hasSize(1);
            ResultPartitionBytes partitionBytes =
                    ioMetrics.getResultPartitionBytes().values().iterator().next();
            assertThat(partitionBytes.getSubpartitionBytes())
                    .containsExactly(bufferSize, bufferSize);
        }
    }

    private TieredStoreResultPartition createTieredStoreResultPartition(
            int numSubpartitions, BufferPool bufferPool, boolean isBroadcastOnly)
            throws IOException {
        TieredStoreResultPartition tieredStoreResultPartition =
                new TieredStoreResultPartition(
                        "TieredStoreResultPartitionTest",
                        0,
                        new ResultPartitionID(),
                        ResultPartitionType.HYBRID_SELECTIVE,
                        numSubpartitions,
                        numSubpartitions,
                        new ResultPartitionManager(),
                        isBroadcastOnly,
                        new TierStorage[0],
                        new BufferCompressor(bufferSize, "LZ4"),
                        new TestingBufferAccumulator(),
                        () -> bufferPool);
        taskIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup();
        tieredStoreResultPartition.setup();
        tieredStoreResultPartition.setMetricGroup(taskIOMetricGroup);
        return tieredStoreResultPartition;
    }
}
