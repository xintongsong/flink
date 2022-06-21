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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Default implementation of {@link HsFileDataIndex}. */
@ThreadSafe
public class HsFileDataIndexImpl implements HsFileDataIndex {

    @GuardedBy("lock")
    private final List<TreeMap<Integer, InternalRegion>>
            subpartition_firstBufferIndex_internalRegions;

    private final Object lock = new Object();

    public HsFileDataIndexImpl(int numSubpartitions) {
        this.subpartition_firstBufferIndex_internalRegions = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartition_firstBufferIndex_internalRegions.add(new TreeMap<>());
        }
    }

    @Override
    public Optional<ReadableRegion> getReadableRegion(int subpartitionId, int bufferIndex) {
        synchronized (lock) {
            return getInternalRegion(subpartitionId, bufferIndex)
                    .map(internalRegion -> internalRegion.toReadableRegion(bufferIndex))
                    .filter(internalRegion -> internalRegion.numReadable > 0);
        }
    }

    @Override
    public void addBuffers(List<SpilledBuffer> spilledBuffers) {
        final Map<Integer, List<InternalRegion>> subpartition_internalRegions =
                convertToInternalRegions(spilledBuffers);
        synchronized (lock) {
            subpartition_internalRegions.forEach(
                    (subpartition, internalRegions) -> {
                        TreeMap<Integer, InternalRegion> treeMap =
                                subpartition_firstBufferIndex_internalRegions.get(subpartition);
                        for (InternalRegion internalRegion : internalRegions) {
                            treeMap.put(internalRegion.firstBufferIndex, internalRegion);
                        }
                    });
        }
    }

    @Override
    public void markBufferReadable(int subpartitionId, int bufferIndex) {
        synchronized (lock) {
            getInternalRegion(subpartitionId, bufferIndex)
                    .ifPresent(internalRegion -> internalRegion.markBufferReadable(bufferIndex));
        }
    }

    @GuardedBy("lock")
    private Optional<InternalRegion> getInternalRegion(int subpartitionId, int bufferIndex) {
        return Optional.ofNullable(
                        subpartition_firstBufferIndex_internalRegions
                                .get(subpartitionId)
                                .floorEntry(bufferIndex))
                .map(Map.Entry::getValue)
                .filter(internalRegion -> internalRegion.containBuffer(bufferIndex));
    }

    private static Map<Integer, List<InternalRegion>> convertToInternalRegions(
            List<SpilledBuffer> spilledBuffers) {

        if (spilledBuffers.isEmpty()) {
            return Collections.emptyMap();
        }

        // There's at least on buffer

        final Map<Integer, List<InternalRegion>> internalRegionsBySubpartition = new HashMap<>();
        final Iterator<SpilledBuffer> iterator = spilledBuffers.iterator();
        SpilledBuffer firstBufferOfCurrentRegion = iterator.next();
        SpilledBuffer lastBufferOfCurrentRegion = firstBufferOfCurrentRegion;

        while (iterator.hasNext()) {
            SpilledBuffer currentBuffer = iterator.next();

            if (currentBuffer.subpartitionId != firstBufferOfCurrentRegion.subpartitionId
                    || currentBuffer.bufferIndex != lastBufferOfCurrentRegion.bufferIndex + 1) {
                // the current buffer belongs to a new region, close the previous region
                addInternalRegionToMap(
                        firstBufferOfCurrentRegion,
                        lastBufferOfCurrentRegion,
                        internalRegionsBySubpartition);
                firstBufferOfCurrentRegion = currentBuffer;
            }

            lastBufferOfCurrentRegion = currentBuffer;
        }

        // close the last region
        addInternalRegionToMap(
                firstBufferOfCurrentRegion,
                lastBufferOfCurrentRegion,
                internalRegionsBySubpartition);

        return internalRegionsBySubpartition;
    }

    private static void addInternalRegionToMap(
            SpilledBuffer firstBufferInRegion,
            SpilledBuffer lastBufferInRegion,
            Map<Integer, List<InternalRegion>> internalRegionsBySubpartition) {
        checkArgument(firstBufferInRegion.subpartitionId == lastBufferInRegion.subpartitionId);
        checkArgument(firstBufferInRegion.bufferIndex <= lastBufferInRegion.bufferIndex);
        internalRegionsBySubpartition
                .computeIfAbsent(firstBufferInRegion.subpartitionId, ArrayList::new)
                .add(
                        new InternalRegion(
                                firstBufferInRegion.bufferIndex,
                                firstBufferInRegion.fileOffset,
                                lastBufferInRegion.bufferIndex
                                        - firstBufferInRegion.bufferIndex
                                        + 1));
    }

    /**
     * In hybrid shuffle mode, buffer may be persisted into a file, and all subpartition's buffer
     * share the same file. Depending on the spilling strategy, the file may not contain all the
     * buffers of a subpartition.Each buffer is uniquely identified by its subpartition id and index
     * within the subpartition.
     *
     * <p>Logically: Each subpartition has its own buffers with increasing buffer index
     * independently.
     *
     * <p>Physically, buffers can be stored in any order. That means, 1) order of buffers from the
     * same subpartition may not be preserved, and 2) buffers from different subpartitions are mixed
     * together.
     *
     * <p>For example:
     *
     * <p>Logically:
     *
     * <p>Note that 2-3 indicates the buffer with index 3 in subpartition 2.
     *
     * <ul>
     *   <li>Subpartition1 : 1-1, 1-2, 1-4, 1-5
     *   <li>Subpartition2 : 2-3, 2-4, 2-6, 2-7
     *   <li>Subpartition3 : 3-2, 3-3, 3-4, 3-5
     * </ul>
     *
     * <p>Physically:
     *
     * <p>(1-1), (1-2), (2-3), (3-2), (3-3), (1-4), (1-5), (2-4), (2-6), (2-7), (3-4), (3-5)
     *
     * <p>To better index data on disks, {@link InternalRegion} is introduced, which is a logical
     * concept that represents a collection of buffers belong to the same subpartition on the disk.
     * Each subpartition will form multiple splits according to the continuity of the buffer index.
     *
     * <p>For the example above, the following splits would be formed. Note that each of the
     * brackets indicates that these buffers belong to a single split.
     *
     * <p>(1-1, 1-2), (2-3), (3-2, 3-3), (1-4, 1-5), (2-4), (2-6, 2-7), (3-4, 3-5)
     *
     * <p>One point to note: Continuous buffers belonging to the same subpartition on disk may also
     * belong to different splits.
     */
    private static class InternalRegion {
        /** The index of the split's first buffer. */
        private final int firstBufferIndex;

        /** The offset in the shuffle file of the split's first buffer. */
        private final long firstBufferOffset;

        /** The number of buffers in this split. */
        private final int numBuffers;

        /**
         * To mark whether each buffer is released, the subscript of the array represents the
         * relative index of the corresponding buffer to the first buffer index.
         */
        private final boolean[] readable;

        private InternalRegion(int firstBufferIndex, long firstBufferOffset, int numBuffers) {
            this.firstBufferIndex = firstBufferIndex;
            this.firstBufferOffset = firstBufferOffset;
            this.numBuffers = numBuffers;
            this.readable = new boolean[numBuffers];
            Arrays.fill(readable, false);
        }

        private boolean containBuffer(int bufferIndex) {
            return bufferIndex >= firstBufferIndex && bufferIndex < firstBufferIndex + numBuffers;
        }

        private HsFileDataIndex.ReadableRegion toReadableRegion(int bufferIndex) {
            int nSkip = bufferIndex - firstBufferIndex;
            int nReadable = 0;
            while (nSkip + nReadable < numBuffers) {
                if (!readable[nSkip + nReadable]) {
                    break;
                }
                ++nReadable;
            }
            return new ReadableRegion(nSkip, nReadable, firstBufferOffset);
        }

        private void markBufferReadable(int bufferIndex) {
            readable[bufferIndex - firstBufferIndex] = true;
        }
    }
}
