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

package org.apache.flink.streaming.runtime.operators.exercise;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.TestKeyedInternalTimerService;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.exercise.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.runtime.operators.exercise.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.runtime.operators.exercise.assigners.WindowAssigner;
import org.apache.flink.streaming.runtime.operators.exercise.triggers.EventTimeTrigger;
import org.apache.flink.streaming.runtime.operators.exercise.triggers.Trigger;
import org.apache.flink.streaming.runtime.operators.exercise.triggers.TriggerResult;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link WindowOperator}.
 */
public class WindowOperatorTest {

	private static final int WINDOW_SIZE_MS = 10;
	private static final int WINDOW_SLIDE_MS = 5;
	private static final String KEY = "testing_key";

	@Test
	public void testEmittedDataHasEndOfWindowTimestamp() throws Exception {
		WindowAssigner<Object> windowAssigner = TumblingEventTimeWindows.of(Time.of(WINDOW_SIZE_MS, TimeUnit.MILLISECONDS));
		Trigger<Object> trigger = new EventTimeTrigger();
		List<Iterable<Object>> emittedData = new ArrayList<>();

		TestWindowOperator<String, Object> windowOperator =
			createWindowOperator(windowAssigner, trigger, tuple3 -> emittedData.add(tuple3.f2));

		List<StreamRecord<Object>> dataToProcess = IntStream.range(0, WINDOW_SIZE_MS + 1)
			.boxed()
			.map(t -> new StreamRecord<Object>(t, t))
			.collect(Collectors.toList());
		List<Object> expectedData = IntStream.range(0, WINDOW_SIZE_MS)
			.boxed()
			.collect(Collectors.toList());

		windowOperator.open();

		windowOperator.setStreamOperatorCurrentKey(KEY);
		for (StreamRecord<Object> record : dataToProcess) {
			windowOperator.processElement(record);
		}

		windowOperator.getKeyedInternalTimerService().advanceWatermark(WINDOW_SIZE_MS);

		assertThat(emittedData.size(), is(1));
		assertThat(emittedData.get(0), is(expectedData));
	}

	@Test
	public void testAssignersToMultipleWindows() throws Exception {
		WindowAssigner<Object> windowAssigner = SlidingEventTimeWindows.of(
			Time.of(WINDOW_SIZE_MS, TimeUnit.MILLISECONDS), Time.of(WINDOW_SLIDE_MS, TimeUnit.MILLISECONDS));
		Trigger<Object> trigger = new EventTimeTrigger();
		List<Iterable<Object>> emittedData = new ArrayList<>();

		TestWindowOperator<String, Object> windowOperator =
			createWindowOperator(windowAssigner, trigger, tuple3 -> emittedData.add(tuple3.f2));

		List<StreamRecord<Object>> dataToProcess = IntStream.range(0, WINDOW_SIZE_MS + WINDOW_SLIDE_MS + 1)
			.boxed()
			.map(t -> new StreamRecord<Object>(t, t))
			.collect(Collectors.toList());
		List<Object> expectedData1 = IntStream.range(0, WINDOW_SLIDE_MS)
			.boxed()
			.collect(Collectors.toList());
		List<Object> expectedData2 = IntStream.range(0, WINDOW_SIZE_MS)
			.boxed()
			.collect(Collectors.toList());
		List<Object> expectedData3 = IntStream.range(WINDOW_SLIDE_MS, WINDOW_SIZE_MS + WINDOW_SLIDE_MS)
			.boxed()
			.collect(Collectors.toList());

		windowOperator.open();

		windowOperator.setStreamOperatorCurrentKey(KEY);
		for (StreamRecord<Object> record : dataToProcess) {
			windowOperator.processElement(record);
		}

		windowOperator.getKeyedInternalTimerService().advanceWatermark(WINDOW_SIZE_MS + WINDOW_SLIDE_MS);

		assertThat(emittedData.size(), is(3));
		assertThat(emittedData.get(0), is(expectedData1));
		assertThat(emittedData.get(1), is(expectedData2));
		assertThat(emittedData.get(2), is(expectedData3));
	}

	@Test
	public void testAssignersToZeroWindows() throws Exception {
		WindowAssigner<Object> windowAssigner = (WindowAssigner<Object>) (element, timestamp, context) -> Collections.emptyList();
		Trigger<Object> trigger = new EventTimeTrigger();
		List<Iterable<Object>> emittedData = new ArrayList<>();

		TestWindowOperator<String, Object> windowOperator =
			createWindowOperator(windowAssigner, trigger, tuple3 -> emittedData.add(tuple3.f2));

		List<StreamRecord<Object>> dataToProcess = IntStream.range(0, WINDOW_SIZE_MS + 1)
			.boxed()
			.map(t -> new StreamRecord<Object>(t, t))
			.collect(Collectors.toList());

		windowOperator.open();

		windowOperator.setStreamOperatorCurrentKey(KEY);
		for (StreamRecord<Object> record : dataToProcess) {
			windowOperator.processElement(record);
		}

		windowOperator.getKeyedInternalTimerService().advanceWatermark(WINDOW_SIZE_MS);

		assertThat(emittedData.size(), is(0));
	}

	@Test
	public void deleteTimerWhenWindowPurged() throws Exception {
		WindowAssigner<Object> windowAssigner = TumblingEventTimeWindows.of(Time.of(WINDOW_SIZE_MS, TimeUnit.MILLISECONDS));
		Trigger<Object> trigger = new EventTimeTrigger() {
			@Override
			public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
				return TriggerResult.PURGE;
			}
		};

		TestWindowOperator<String, Object> windowOperator =
			createWindowOperator(windowAssigner, trigger, ignore -> {});

		List<StreamRecord<Object>> dataToProcess = IntStream.range(0, WINDOW_SIZE_MS)
			.boxed()
			.map(t -> new StreamRecord<Object>(t, t))
			.collect(Collectors.toList());

		windowOperator.open();

		windowOperator.setStreamOperatorCurrentKey(KEY);
		for (StreamRecord<Object> record : dataToProcess) {
			windowOperator.processElement(record);
		}

		windowOperator.getKeyedInternalTimerService().advanceWatermark(WINDOW_SIZE_MS);
		assertThat(windowOperator.getKeyedInternalTimerService().numEventTimeTimers(), is(0));
	}

	public static TestWindowOperator<String, Object> createWindowOperator(
		WindowAssigner<? super Object> windowAssigner,
		Trigger<? super Object> trigger,
		Consumer<Tuple3<String, TimeWindow, Iterable<Object>>> windowFunctionConsumer) {

		return new TestWindowOperator<>(
			(WindowFunction<Object, Object, String, TimeWindow>) (key, win, in, out) -> windowFunctionConsumer.accept(Tuple3.of(key, win, in)),
			windowAssigner,
			trigger,
			new ListStateDescriptor<>("ignore", TypeInformation.of(new TypeHint<Object>(){}).createSerializer(new ExecutionConfig())));
	}

	private static class TestWindowOperator<K, IN> extends WindowOperator<K, IN, Object> {
		private TestWindowState<K, IN> windowState;
		private TestKeyedInternalTimerService<K, TimeWindow> keyedInternalTimerService;

		private TestWindowOperator(
			WindowFunction<IN, Object, K, TimeWindow> windowFunction,
			WindowAssigner<? super IN> windowAssigner,
			Trigger<? super IN> trigger,
			StateDescriptor<ListState<IN>, ?> windowStateDescriptor) {
			super(windowFunction,
				windowAssigner,
				trigger,
				windowStateDescriptor);
		}

		public TestKeyedInternalTimerService<K, TimeWindow> getKeyedInternalTimerService() {
			return keyedInternalTimerService;
		}

		@Override
		public void open() {
			windowState = new TestWindowState<>();
			keyedInternalTimerService = new TestKeyedInternalTimerService<>(this);

			final TriggerContextImpl<K> triggerContext = new TriggerContextImpl<>(keyedInternalTimerService);
			final WindowAssigner.WindowAssignerContext assignerContext = keyedInternalTimerService::currentProcessingTime;

			setUpContext(windowState, triggerContext, assignerContext);
		}

		@Override
		public void setStreamOperatorCurrentKey(K key) {
			windowState.setCurrentKey(key);
		}

		@Override
		public K getStreamOperatorCurrentKey() {
			return windowState.getCurrentKey();
		}
	}

	private static class TestWindowState<K, IN> implements InternalListState<K, TimeWindow, IN> {

		private K currentKey;
		private TimeWindow currentNamespace;
		private Map<K, Map<TimeWindow, List<IN>>> states = new HashMap<>();

		public void setCurrentKey(K key) {
			currentKey = key;
		}

		public K getCurrentKey() {
			return currentKey;
		}

		public void add(K key, TimeWindow namespace, IN value) throws Exception {
			Map<TimeWindow, List<IN>> statesOfKey = states.computeIfAbsent(key, k -> new HashMap<>());
			List<IN> statesOfNamespace = statesOfKey.computeIfAbsent(namespace, k -> new ArrayList<>());
			statesOfNamespace.add(value);
		}

		public Iterable<IN> get(K key, TimeWindow namespace) {
			Map<TimeWindow, List<IN>> statesOfKey = states.get(key);
			if (statesOfKey == null) {
				return null;
			}

			return statesOfKey.get(namespace);
		}

		public void clear(K key, TimeWindow namespace) {
			Map<TimeWindow, List<IN>> statesOfKey = states.get(key);
			if (statesOfKey == null) {
				return;
			}

			statesOfKey.remove(namespace);

			if (statesOfKey.isEmpty()) {
				states.remove(key);
			}
		}

		@Override
		public void setCurrentNamespace(TimeWindow namespace) {
			currentNamespace = namespace;
		}

		@Override
		public void add(IN value) throws Exception {
			add(currentKey, currentNamespace, value);
		}

		@Override
		public Iterable<IN> get() throws Exception {
			return get(currentKey, currentNamespace);
		}

		@Override
		public void clear() {
			clear(currentKey, currentNamespace);
		}

		@Override
		public void update(List<IN> values) throws Exception {

		}

		@Override
		public void addAll(List<IN> values) throws Exception {

		}

		@Override
		public void mergeNamespaces(TimeWindow target, Collection<TimeWindow> sources) throws Exception {

		}

		@Override
		public List<IN> getInternal() throws Exception {
			return null;
		}

		@Override
		public void updateInternal(List<IN> valueToStore) throws Exception {

		}

		@Override
		public TypeSerializer<K> getKeySerializer() {
			return null;
		}

		@Override
		public TypeSerializer<TimeWindow> getNamespaceSerializer() {
			return null;
		}

		@Override
		public TypeSerializer<List<IN>> getValueSerializer() {
			return null;
		}

		@Override
		public byte[] getSerializedValue(
			byte[] serializedKeyAndNamespace,
			TypeSerializer<K> safeKeySerializer,
			TypeSerializer<TimeWindow> safeNamespaceSerializer,
			TypeSerializer<List<IN>> safeValueSerializer) throws Exception {
			return new byte[0];
		}

		@Override
		public StateIncrementalVisitor<K, TimeWindow, List<IN>> getStateIncrementalVisitor(
			int recommendedMaxNumberOfReturnedRecords) {
			return null;
		}
	}
}
