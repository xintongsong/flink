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
import org.apache.flink.util.function.ConsumerFunction;

public interface NonKeyedPartitionStream<T> {

    <OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> process(
            SingleStreamProcessFunction<T, OUT> processFunction);

    <OUT1, OUT2> ProcessConfigurableAndTwoNonKeyedPartitionStreams<OUT1, OUT2> process(
            TwoOutputStreamProcessFunction<T, OUT1, OUT2> processFunction);

    <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            NonKeyedPartitionStream<T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction);

    <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            BroadcastStream<T_OTHER> other,
            TwoInputStreamProcessFunction<T, T_OTHER, OUT> processFunction);

    GlobalStream<T> coalesce();

    <K> KeyedPartitionStream<K, T> keyBy(KeySelector<T, K> keySelector);

    NonKeyedPartitionStream<T> shuffle();

    BroadcastStream<T> broadcast();

    ProcessConfigurable<?> sinkTo(Sink<T> sink);
    /** TODO: Temporal method. Will revisit sink functions later. */
    void tmpToConsumerSink(ConsumerFunction<T> consumer);

    interface ProcessConfigurableAndNonKeyedPartitionStream<T>
            extends NonKeyedPartitionStream<T>,
                    ProcessConfigurable<ProcessConfigurableAndNonKeyedPartitionStream<T>> {}

    interface ProcessConfigurableAndTwoNonKeyedPartitionStreams<T1, T2>
            extends ProcessConfigurable<ProcessConfigurableAndTwoNonKeyedPartitionStreams<T1, T2>> {
        NonKeyedPartitionStream<T1> getFirst();

        NonKeyedPartitionStream<T2> getSecond();
    }
}
