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

package org.apache.flink.processfunction.api.stream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.processfunction.api.Sink;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.processfunction.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStreams;

public interface KeyedPartitionStream<K, T> {
    /**
     * This method is used to avoid shuffle after applying the process function. It is required that
     * for the same record, the new {@link KeySelector} must extract the same key as the {@link
     * KeySelector} on this {@link KeyedPartitionStream}.
     */
    <OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> process(
            SingleStreamProcessFunction<T, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector);

    <OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> process(
            SingleStreamProcessFunction<T, OUT> processFunction);

    <OUT1, OUT2> ProcessConfigurableAndTwoKeyedPartitionStreams<K, OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction,
            KeySelector<OUT1, K> keySelector1,
            KeySelector<OUT2, K> keySelector2);

    <OUT1, OUT2> ProcessConfigurableAndTwoNonKeyedPartitionStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction);

    /** Keyed connect to Keyed. */
    <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction);

    /**
     * Keyed connect to Keyed. This method is used to avoid shuffle after applying the process
     * function. It is required that for the same record, the new {@link KeySelector} must extract
     * the same key as the {@link KeySelector} on these two {@link KeyedPartitionStream}s.
     */
    <T_OTHER, OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector);

    /** Keyed connect to Broadcast. */
    <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction);

    GlobalStream<T> coalesce();

    <NEW_KEY> KeyedPartitionStream<NEW_KEY, T> keyBy(KeySelector<T, NEW_KEY> keySelector);

    NonKeyedPartitionStream<T> shuffle();

    BroadcastStream<T> broadcast();

    ProcessConfigurable<?> sinkTo(Sink<T> sink);

    // TODO implements two output process return keyed stream.
    interface TwoOutputStreams<K, T1, T2> {
        KeyedPartitionStream<K, T1> getFirst();

        KeyedPartitionStream<K, T2> getSecond();
    }

    interface ProcessConfigurableAndKeyedPartitionStream<K, T>
            extends KeyedPartitionStream<K, T>,
                    ProcessConfigurable<ProcessConfigurableAndKeyedPartitionStream<K, T>> {}

    interface ProcessConfigurableAndTwoKeyedPartitionStreams<K, T1, T2>
            extends ProcessConfigurable<ProcessConfigurableAndTwoKeyedPartitionStreams<K, T1, T2>> {
        KeyedPartitionStream<K, T1> getFirst();

        KeyedPartitionStream<K, T2> getSecond();
    }
}
