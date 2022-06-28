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

import java.time.Duration;

/** Configuration for hybrid shuffle mode. */
public class HybridShuffleConfiguration {
    /**
     * Determine how many buffers to read ahead at most for each subpartition to prevent other
     * consumers from starving.
     */
    private final int maxBuffersReadAhead;

    /**
     * Maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     */
    private final Duration bufferRequestTimeout;

    // one partition reader can consume at most Math.max(16M, numSubpartitions) (the expected
    // buffers per request is 8M) buffers for data read, which means larger parallelism, more
    // buffers. Currently, it is only an empirical strategy which can not be configured.
    private final int maxRequestedBuffers;

    public HybridShuffleConfiguration(
            int maxBuffersReadAhead, Duration bufferRequestTimeout, int maxRequestedBuffers) {
        this.maxBuffersReadAhead = maxBuffersReadAhead;
        this.bufferRequestTimeout = bufferRequestTimeout;
        this.maxRequestedBuffers = maxRequestedBuffers;
    }

    /**
     * Temporarily adopt a fixed value, and then adjust the default value according to the
     * experiment, and introduce configuration options.
     */
    public static HybridShuffleConfiguration createConfiguration(
            int numSubpartitions, int numBuffersPerRequest) {
        final int maxBuffersReadAhead = 5;
        final Duration bufferRequestTimeout = Duration.ofMinutes(5);
        final int maxRequestedBuffers = Math.max(2 * numBuffersPerRequest, numSubpartitions);
        return new HybridShuffleConfiguration(
                maxBuffersReadAhead, bufferRequestTimeout, maxRequestedBuffers);
    }

    public int getMaxRequestedBuffers() {
        return maxRequestedBuffers;
    }

    public int getMaxBuffersReadAhead() {
        return maxBuffersReadAhead;
    }

    public Duration getBufferRequestTimeout() {
        return bufferRequestTimeout;
    }
}
