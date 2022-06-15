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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndex.SpilledBuffer;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsMemoryDataSpiller}. */
@ExtendWith(TestLoggerExtension.class)
class HsMemoryDataSpillerTest {

    private final int bufferSize = 1024;

    private final byte[] dataBytes = new byte[bufferSize];

    private FileChannel dataFileChannel;

    private HsMemoryDataSpiller memoryDataSpiller;

    @BeforeEach
    void before(@TempDir Path tempDir) throws Exception {
        Random random = new Random();
        random.nextBytes(dataBytes);
        dataFileChannel =
                FileChannel.open(
                        Files.createFile(tempDir.resolve(".data")), StandardOpenOption.WRITE);
        this.memoryDataSpiller = new HsMemoryDataSpiller(dataFileChannel);
    }

    @Test
    void testSpillExceptionally() throws IOException {
        int targetChannel = 0;
        List<BufferWithIdentity> bufferWithIdentityList =
                createBufferWithIdentityList(targetChannel, 0, 1, 2);
        // close data file channel to trigger exception when spill data into disk.
        dataFileChannel.close();

        CompletableFuture<List<SpilledBuffer>> future =
                memoryDataSpiller.spillAsync(bufferWithIdentityList);
        assertThat(future)
                .failsWithin(60, TimeUnit.SECONDS)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(ClosedChannelException.class);
    }

    @Test
    void testSpillSuccessfully() {
        List<BufferWithIdentity> bufferWithIdentityList = createBufferWithIdentityList(0, 1, 2, 4);
        CompletableFuture<List<SpilledBuffer>> future =
                memoryDataSpiller.spillAsync(bufferWithIdentityList);
        assertThat(future)
                .succeedsWithin(60, TimeUnit.SECONDS)
                .satisfies(list -> assertThat(list).hasSize(3));
    }

    private List<BufferWithIdentity> createBufferWithIdentityList(
            int targetChannel, int... bufferIndexes) {
        List<BufferWithIdentity> bufferWithIdentityList = new ArrayList<>();
        for (int index : bufferIndexes) {
            Buffer.DataType dataType =
                    index % 2 == 0 ? Buffer.DataType.EVENT_BUFFER : Buffer.DataType.DATA_BUFFER;

            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
            segment.put(0, dataBytes);
            Buffer buffer =
                    new NetworkBuffer(
                            segment, FreeingBufferRecycler.INSTANCE, dataType, bufferSize);
            bufferWithIdentityList.add(new BufferWithIdentity(buffer, index, targetChannel));
        }
        return Collections.unmodifiableList(bufferWithIdentityList);
    }
}
