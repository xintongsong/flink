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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered2.shuffle;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class TieredInputGate extends IndexedInputGate {
    @Override
    public int getGateIndex() {
        return 0;
    }

    @Override
    public List<InputChannelInfo> getUnfinishedChannels() {
        return null;
    }

    @Override
    public void triggerDebloating() {}

    @Override
    public int getNumberOfInputChannels() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public EndOfDataStatus hasReceivedEndOfData() {
        return null;
    }

    @Override
    public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
        return Optional.empty();
    }

    @Override
    public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
        return Optional.empty();
    }

    @Override
    public void sendTaskEvent(TaskEvent event) throws IOException {}

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {}

    @Override
    public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {}

    @Override
    public InputChannel getChannel(int channelIndex) {
        return null;
    }

    @Override
    public void setup() throws IOException {}

    @Override
    public void requestPartitions() throws IOException {}

    @Override
    public CompletableFuture<Void> getStateConsumedFuture() {
        return null;
    }

    @Override
    public void finishReadRecoveredState() throws IOException {}

    @Override
    public void close() throws Exception {}
}
