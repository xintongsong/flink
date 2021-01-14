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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import java.util.concurrent.Executor;

/** Builder for {@link FineGrainedSlotManager}. */
public class FineGrainedSlotManagerBuilder {
    private ResourceAllocationStrategy resourceAllocationStrategy;
    private ScheduledExecutor scheduledExecutor;
    private Time taskManagerRequestTimeout;
    private Time slotRequestTimeout;
    private Time taskManagerTimeout;
    private boolean waitResultConsumedBeforeRelease;
    private WorkerResourceSpec defaultWorkerResourceSpec;
    private int numSlotsPerWorker;
    private SlotManagerMetricGroup slotManagerMetricGroup;
    private int maxSlotNum;
    private int redundantTaskManagerNum;
    private ResourceTracker resourceTracker;
    private TaskManagerTracker taskManagerTracker;
    private SlotStatusSyncer slotStatusSyncer;
    private Executor executor;

    private FineGrainedSlotManagerBuilder() {
        this.scheduledExecutor = TestingUtils.defaultScheduledExecutor();
        this.taskManagerRequestTimeout = TestingUtils.infiniteTime();
        this.slotRequestTimeout = TestingUtils.infiniteTime();
        this.taskManagerTimeout = TestingUtils.infiniteTime();
        this.waitResultConsumedBeforeRelease = true;
        this.defaultWorkerResourceSpec = WorkerResourceSpec.ZERO;
        this.numSlotsPerWorker = 1;
        this.slotManagerMetricGroup =
                UnregisteredMetricGroups.createUnregisteredSlotManagerMetricGroup();
        this.maxSlotNum = ResourceManagerOptions.MAX_SLOT_NUM.defaultValue();
        this.redundantTaskManagerNum =
                ResourceManagerOptions.REDUNDANT_TASK_MANAGER_NUM.defaultValue();
        this.resourceTracker = new DefaultResourceTracker();
        this.taskManagerTracker = new FineGrainedTaskManagerTracker();
        this.slotStatusSyncer = new DefaultSlotStatusSyncer(TestingUtils.infiniteTime());
        this.executor = Executors.directExecutor();
    }

    public static FineGrainedSlotManagerBuilder newBuilder() {
        return new FineGrainedSlotManagerBuilder();
    }

    public FineGrainedSlotManagerBuilder setScheduledExecutor(ScheduledExecutor scheduledExecutor) {
        this.scheduledExecutor = scheduledExecutor;
        return this;
    }

    public FineGrainedSlotManagerBuilder setResourceTracker(ResourceTracker resourceTracker) {
        this.resourceTracker = resourceTracker;
        return this;
    }

    public FineGrainedSlotManagerBuilder setTaskManagerTracker(
            TaskManagerTracker taskManagerTracker) {
        this.taskManagerTracker = taskManagerTracker;
        return this;
    }

    public FineGrainedSlotManagerBuilder setSlotStatusSyncer(SlotStatusSyncer slotStatusSyncer) {
        this.slotStatusSyncer = slotStatusSyncer;
        return this;
    }

    public FineGrainedSlotManagerBuilder setResourceAllocationStrategy(
            ResourceAllocationStrategy resourceAllocationStrategy) {
        this.resourceAllocationStrategy = resourceAllocationStrategy;
        return this;
    }

    public FineGrainedSlotManagerBuilder setTaskManagerRequestTimeout(
            Time taskManagerRequestTimeout) {
        this.taskManagerRequestTimeout = taskManagerRequestTimeout;
        return this;
    }

    public FineGrainedSlotManagerBuilder setSlotRequestTimeout(Time slotRequestTimeout) {
        this.slotRequestTimeout = slotRequestTimeout;
        return this;
    }

    public FineGrainedSlotManagerBuilder setTaskManagerTimeout(Time taskManagerTimeout) {
        this.taskManagerTimeout = taskManagerTimeout;
        return this;
    }

    public FineGrainedSlotManagerBuilder setWaitResultConsumedBeforeRelease(
            boolean waitResultConsumedBeforeRelease) {
        this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
        return this;
    }

    public FineGrainedSlotManagerBuilder setDefaultWorkerResourceSpec(
            WorkerResourceSpec defaultWorkerResourceSpec) {
        this.defaultWorkerResourceSpec = defaultWorkerResourceSpec;
        return this;
    }

    public FineGrainedSlotManagerBuilder setNumSlotsPerWorker(int numSlotsPerWorker) {
        this.numSlotsPerWorker = numSlotsPerWorker;
        return this;
    }

    public FineGrainedSlotManagerBuilder setSlotManagerMetricGroup(
            SlotManagerMetricGroup slotManagerMetricGroup) {
        this.slotManagerMetricGroup = slotManagerMetricGroup;
        return this;
    }

    public FineGrainedSlotManagerBuilder setMaxSlotNum(int maxSlotNum) {
        this.maxSlotNum = maxSlotNum;
        return this;
    }

    public FineGrainedSlotManagerBuilder setRedundantTaskManagerNum(int redundantTaskManagerNum) {
        this.redundantTaskManagerNum = redundantTaskManagerNum;
        return this;
    }

    public FineGrainedSlotManagerBuilder setExecutor(Executor executor) {
        this.executor = executor;
        return this;
    }

    public FineGrainedSlotManager build() {
        if (resourceAllocationStrategy == null) {
            resourceAllocationStrategy =
                    new DefaultResourceAllocationStrategy(
                            SlotManagerUtils.generateDefaultSlotResourceProfile(
                                    defaultWorkerResourceSpec, numSlotsPerWorker),
                            numSlotsPerWorker);
        }

        final SlotManagerConfiguration slotManagerConfiguration =
                new SlotManagerConfiguration(
                        taskManagerRequestTimeout,
                        slotRequestTimeout,
                        taskManagerTimeout,
                        waitResultConsumedBeforeRelease,
                        AnyMatchingSlotMatchingStrategy.INSTANCE,
                        defaultWorkerResourceSpec,
                        numSlotsPerWorker,
                        maxSlotNum,
                        redundantTaskManagerNum);

        return new FineGrainedSlotManager(
                scheduledExecutor,
                slotManagerConfiguration,
                slotManagerMetricGroup,
                resourceTracker,
                taskManagerTracker,
                slotStatusSyncer,
                resourceAllocationStrategy);
    }

    public FineGrainedSlotManager buildAndStartWithDirectExec() {
        final FineGrainedSlotManager slotManager = build();
        slotManager.start(
                ResourceManagerId.generate(),
                executor,
                new TestingResourceActionsBuilder().build());
        return slotManager;
    }

    public FineGrainedSlotManager buildAndStartWithDirectExec(
            ResourceManagerId resourceManagerId, ResourceActions resourceManagerActions) {
        final FineGrainedSlotManager slotManager = build();
        slotManager.start(resourceManagerId, executor, resourceManagerActions);
        return slotManager;
    }
}
