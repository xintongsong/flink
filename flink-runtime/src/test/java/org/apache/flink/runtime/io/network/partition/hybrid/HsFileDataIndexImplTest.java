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

import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndex.ReadableRegion;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndex.SpilledBuffer;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsFileDataIndexImpl}. */
@ExtendWith(TestLoggerExtension.class)
class HsFileDataIndexImplTest {
    private static final int NUM_SUBPARTITIONS = 3;

    private static final int BUFFER_WITH_HEAD_SIZE = 16;

    private HsFileDataIndexImpl hsDataIndex;

    private long currentOffset;

    @BeforeEach
    void before() {
        hsDataIndex = new HsFileDataIndexImpl(NUM_SUBPARTITIONS);
        currentOffset = 0L;
    }

    /**
     * If the buffer index to be queried is less than all buffers in the corresponding subpartition
     * in the data index, the return value should be {@link Optional#empty()}.
     */
    @Test
    void testLookUpBufferIndexLessThenAllBuffers() {
        // spilled buffers for subpartition 0: (0-3, 0-4, 0-5)
        // note that: 0-3 indicate buffer within subpartition-0 and buffer index is 3.
        List<SpilledBuffer> spilledBuffers = createContinuousSpilledBuffers(0, 3, 3);
        // spilled buffers for subpartition 1: (1-0, 1-1)
        spilledBuffers.addAll(createContinuousSpilledBuffers(1, 0, 2));

        hsDataIndex.addBuffers(spilledBuffers);

        assertThat(hsDataIndex.getReadableRegion(0, 0)).isNotPresent();
        assertThat(hsDataIndex.getReadableRegion(0, 1)).isNotPresent();
        assertThat(hsDataIndex.getReadableRegion(0, 2)).isNotPresent();
    }

    /**
     * If a subpartition has no spilled buffer added to the index, all the query results are {@link
     * Optional#empty()}.
     */
    @Test
    void testLookUpEmptySubpartition() {
        assertThat(hsDataIndex.getReadableRegion(0, 0)).isNotPresent();
        assertThat(hsDataIndex.getReadableRegion(1, 10)).isNotPresent();
    }

    /**
     * If there is a region less than the target buffer index in the {@link HsFileDataIndexImpl},
     * but target buffer is not exists in any added regions, {@link Optional#empty()} should be
     * eventually returned.
     */
    @Test
    void testSplitLessThanTargetBufferIndexExists() {
        // spilled buffers for subpartition 0: (0-3, 0-4, 0-5), (0-7, 0-8)
        List<SpilledBuffer> spilledBuffers = createContinuousSpilledBuffers(0, 3, 3);
        spilledBuffers.addAll(createContinuousSpilledBuffers(0, 7, 2));

        hsDataIndex.addBuffers(spilledBuffers);

        final int targetIndex = 6;
        assertThat(hsDataIndex.getReadableRegion(0, targetIndex)).isNotPresent();
    }

    /**
     * The readable region will also contain the unreleased buffers before the target buffer index.
     */
    @Test
    void testBuffersBeforeTargetAreNotReleased() {
        final int targetChannel = 0;
        // spilled buffers for subpartition 0: (0-3, 0-4, 0-5, 0-6)
        List<SpilledBuffer> spilledBuffers = createContinuousSpilledBuffers(0, 3, 4);
        hsDataIndex.addBuffers(spilledBuffers);

        // Release the target buffer and buffers after it.
        hsDataIndex.markBufferReadable(targetChannel, 5);
        hsDataIndex.markBufferReadable(targetChannel, 6);

        Optional<ReadableRegion> regionOpt = hsDataIndex.getReadableRegion(0, 5);
        assertThat(regionOpt)
                .hasValueSatisfying(
                        (fileRegion -> {
                            assertThat(fileRegion.numSkip).isEqualTo(2);
                            assertThat(fileRegion.numReadable).isEqualTo(2);
                            assertThat(fileRegion.offset).isEqualTo(0);
                        }));
    }

    /** If target buffer is not released, {@link Optional#empty()} should be eventually returned. */
    @Test
    void testTargetBufferIsNotReleased() {
        // spilled buffers for subpartition 0: (0-3, 0-4, 0-5, 0-6)
        List<SpilledBuffer> spilledBuffers = createContinuousSpilledBuffers(0, 3, 4);
        hsDataIndex.addBuffers(spilledBuffers);

        Optional<ReadableRegion> regionOpt = hsDataIndex.getReadableRegion(0, 4);
        assertThat(regionOpt).isNotPresent();
    }

    /**
     * The readable region's buffer index should at the range of [region's head, the first buffer
     * after the target buffer that has not been released).
     */
    @Test
    void testBufferAfterTargetAreNotReleased() {
        final int targetChannel = 0;
        // spilled buffers for subpartition 0: (0-3, 0-4, 0-5, 0-6, 0-7)
        List<SpilledBuffer> spilledBuffers = createContinuousSpilledBuffers(0, 3, 5);
        hsDataIndex.addBuffers(spilledBuffers);

        // only release buffer with index 4 and 5.
        hsDataIndex.markBufferReadable(targetChannel, 4);
        hsDataIndex.markBufferReadable(targetChannel, 5);

        Optional<ReadableRegion> regionOpt = hsDataIndex.getReadableRegion(targetChannel, 4);
        assertThat(regionOpt)
                .hasValueSatisfying(
                        (fileRegion -> {
                            assertThat(fileRegion.numReadable).isEqualTo(2);
                            assertThat(fileRegion.numSkip).isEqualTo(1);
                            assertThat(fileRegion.offset).isEqualTo(0);
                        }));
    }

    /** Test the correctness of the logic of converting spilledBuffers to internal region. */
    @Test
    void testConvertToInternalRegions() {
        List<SpilledBuffer> spilledBuffers = new ArrayList<>();
        spilledBuffers.addAll(createSpilledBuffers(0, Arrays.asList(0, 2)));
        spilledBuffers.addAll(createSpilledBuffers(1, Arrays.asList(1, 2, 4, 5)));
        spilledBuffers.addAll(createSpilledBuffers(2, Arrays.asList(0)));

        hsDataIndex.addBuffers(spilledBuffers);
        // release all buffers, it will let readable region and internal region contain the same
        // buffer.
        spilledBuffers.forEach(
                (buffer) ->
                        hsDataIndex.markBufferReadable(buffer.subpartitionId, buffer.bufferIndex));

        // expected region: (0-0).
        // Note that 0-0 indicates buffer with index 0 and subpartition id 1.
        assertThat(hsDataIndex.getReadableRegion(0, 0))
                .isPresent()
                .hasValueSatisfying(
                        (region) -> {
                            assertThat(region.numSkip).isEqualTo(0);
                            assertThat(region.numReadable).isEqualTo(1);
                            assertThat(region.offset).isEqualTo(0);
                        });
        // expected region: (0-2).
        assertThat(hsDataIndex.getReadableRegion(0, 2))
                .isPresent()
                .hasValueSatisfying(
                        (region) -> {
                            assertThat(region.numSkip).isEqualTo(0);
                            assertThat(region.numReadable).isEqualTo(1);
                            assertThat(region.offset).isEqualTo(BUFFER_WITH_HEAD_SIZE);
                        });
        // expected region: (1-1, 1-2).
        assertThat(hsDataIndex.getReadableRegion(1, 1))
                .isPresent()
                .hasValueSatisfying(
                        (region) -> {
                            assertThat(region.numSkip).isEqualTo(0);
                            assertThat(region.numReadable).isEqualTo(2);
                            assertThat(region.offset).isEqualTo(BUFFER_WITH_HEAD_SIZE * 2);
                        });
        // expected region: (1-4, 1-5).
        assertThat(hsDataIndex.getReadableRegion(1, 4))
                .isPresent()
                .hasValueSatisfying(
                        (region) -> {
                            assertThat(region.numSkip).isEqualTo(0);
                            assertThat(region.numReadable).isEqualTo(2);
                            assertThat(region.offset).isEqualTo(BUFFER_WITH_HEAD_SIZE * 4);
                        });
        // expected region: (2-0).
        assertThat(hsDataIndex.getReadableRegion(2, 0))
                .isPresent()
                .hasValueSatisfying(
                        (region) -> {
                            assertThat(region.numSkip).isEqualTo(0);
                            assertThat(region.numReadable).isEqualTo(1);
                            assertThat(region.offset).isEqualTo(BUFFER_WITH_HEAD_SIZE * 6);
                        });
    }

    private List<SpilledBuffer> createContinuousSpilledBuffers(
            int subpartitionId, int firstBufferIndex, int numBuffers) {
        List<Integer> bufferIndexes =
                IntStream.range(firstBufferIndex, firstBufferIndex + numBuffers)
                        .boxed()
                        .collect(Collectors.toList());
        return createSpilledBuffers(subpartitionId, bufferIndexes);
    }

    private List<SpilledBuffer> createSpilledBuffers(
            int subpartitionId, List<Integer> bufferIndexes) {
        List<SpilledBuffer> spilledBuffers = new ArrayList<>();
        for (int bufferIndex : bufferIndexes) {
            spilledBuffers.add(new SpilledBuffer(subpartitionId, bufferIndex++, currentOffset));
            currentOffset += BUFFER_WITH_HEAD_SIZE;
        }
        return spilledBuffers;
    }
}
