/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.SystemExitTrackingSecurityManager;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionUtils;

import akka.pattern.AskTimeoutException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link FineGrainedSlotManager}. */
public class FineGrainedSlotManagerTest extends TestLogger {
    private static final FlinkException TEST_EXCEPTION = new FlinkException("Test exception");
    private static final WorkerResourceSpec DEFAULT_WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(100.0)
                    .setTaskHeapMemoryMB(10000)
                    .setTaskOffHeapMemoryMB(10000)
                    .setNetworkMemoryMB(10000)
                    .setManagedMemoryMB(10000)
                    .build();
    private static final int DEFAULT_NUM_SLOTS_PER_WORKER = 1;
    private static final ResourceProfile DEFAULT_TOTAL_RESOURCE_PROFILE =
            SlotManagerUtils.generateTaskManagerTotalResourceProfile(DEFAULT_WORKER_RESOURCE_SPEC);
    private static final ResourceProfile DEFAULT_SLOT_RESOURCE_PROFILE =
            SlotManagerUtils.generateDefaultSlotResourceProfile(
                    DEFAULT_WORKER_RESOURCE_SPEC, DEFAULT_NUM_SLOTS_PER_WORKER);

    @Test
    public void testCloseAfterSuspendDoesNotThrowException() throws Exception {
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder().buildAndStartWithDirectExec()) {
            slotManager.suspend();
        }
    }

    /** Tests that we can register task manager at the slot manager. */
    @Test
    public void testTaskManagerRegistration() throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);

        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec()) {
            slotManager.registerTaskManager(
                    taskManagerConnection,
                    new SlotReport(),
                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                    DEFAULT_SLOT_RESOURCE_PROFILE);

            assertThat(slotManager.getNumberRegisteredSlots(), equalTo(1));
            assertThat(tracker.getRegisteredTaskManagers().size(), equalTo(1));
            assertTrue(
                    tracker.getRegisteredTaskManager(taskManagerConnection.getInstanceID())
                            .isPresent());
            assertThat(
                    tracker.getRegisteredTaskManager(taskManagerConnection.getInstanceID())
                            .get()
                            .getAvailableResource(),
                    equalTo(DEFAULT_TOTAL_RESOURCE_PROFILE));
            assertThat(
                    tracker.getRegisteredTaskManager(taskManagerConnection.getInstanceID())
                            .get()
                            .getTotalResource(),
                    equalTo(DEFAULT_TOTAL_RESOURCE_PROFILE));
        }
    }

    /** Tests that we can matched task manager will deduct pending task manager. */
    @Test
    public void testTaskManagerRegistrationDeductPendingTaskManager() throws Exception {
        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec()) {

            final TaskExecutorGateway taskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
            final TaskExecutorConnection taskExecutionConnection1 =
                    new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
            final TaskExecutorConnection taskExecutionConnection2 =
                    new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
            final TaskExecutorConnection taskExecutionConnection3 =
                    new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
            final SlotReport slotReportWithAllocatedSlot =
                    new SlotReport(
                            createAllocatedSlotStatus(new AllocationID(), ResourceProfile.ANY));
            tracker.addPendingTaskManager(
                    new PendingTaskManager(
                            PendingTaskManagerId.generate(),
                            ResourceProfile.ANY,
                            ResourceProfile.ANY));
            // task manager with allocated slot cannot deduct pending task manager
            slotManager.registerTaskManager(
                    taskExecutionConnection1,
                    slotReportWithAllocatedSlot,
                    ResourceProfile.ANY,
                    ResourceProfile.ANY);
            assertThat(tracker.getPendingTaskManagers().size(), is(1));
            // task manager with mismatched resource cannot deduct pending task manager
            slotManager.registerTaskManager(
                    taskExecutionConnection2,
                    new SlotReport(),
                    ResourceProfile.fromResources(10, 100),
                    ResourceProfile.fromResources(10, 100));
            assertThat(tracker.getPendingTaskManagers().size(), is(1));
            slotManager.registerTaskManager(
                    taskExecutionConnection3,
                    new SlotReport(),
                    ResourceProfile.ANY,
                    ResourceProfile.ANY);
            assertThat(tracker.getPendingTaskManagers().size(), is(0));
        }
    }

    /** Tests that un-registration of task managers will free and remove all allocated slots. */
    @Test
    public void testTaskManagerUnregistration() throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(tuple6 -> new CompletableFuture<>())
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);

        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final SlotReport slotReport =
                new SlotReport(createAllocatedSlotStatus(allocationId, resourceProfile));

        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTracker(tracker)
                        .setNumSlotsPerWorker(2)
                        .buildAndStartWithDirectExec()) {
            slotManager.registerTaskManager(
                    taskManagerConnection,
                    slotReport,
                    resourceProfile.multiply(2),
                    resourceProfile);

            assertThat(tracker.getRegisteredTaskManagers().size(), is(1));

            final Optional<TaskManagerSlotInformation> slot =
                    tracker.getAllocatedOrPendingSlot(allocationId);
            assertTrue(slot.isPresent());
            assertTrue(slot.get().getState() == SlotState.ALLOCATED);

            slotManager.unregisterTaskManager(
                    taskManagerConnection.getInstanceID(), TEST_EXCEPTION);
            assertThat(tracker.getRegisteredTaskManagers(), is(empty()));
            assertFalse(tracker.getAllocatedOrPendingSlot(allocationId).isPresent());
        }
    }

    /**
     * Tests that a requirement declaration with no free slots will trigger the resource allocation.
     */
    @Test
    public void testRequirementDeclarationWithoutFreeSlotsTriggersWorkerAllocation()
            throws Exception {
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        final CompletableFuture<WorkerResourceSpec> allocateResourceFuture =
                new CompletableFuture<>();
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceConsumer(allocateResourceFuture::complete)
                        .build();

        try (SlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), resourceManagerActions)) {

            slotManager.processResourceRequirements(resourceRequirements);

            allocateResourceFuture.get();
        }
    }

    /**
     * Tests that resources continue to be considered missing if we cannot allocate more resources.
     */
    @Test
    public void testRequirementDeclarationWithResourceAllocationFailure() throws Exception {
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(value -> false)
                        .build();

        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), resourceManagerActions)) {

            slotManager.processResourceRequirements(resourceRequirements);

            final JobID jobId = resourceRequirements.getJobId();
            assertThat(
                    getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)), is(1));
        }
    }

    /** Tests that resource requirements can be fulfilled with resource that are currently free. */
    @Test
    public void testRequirementDeclarationWithFreeResource() throws Exception {
        testRequirementDeclaration(
                RequirementDeclarationScenario
                        .TASK_EXECUTOR_REGISTRATION_BEFORE_REQUIREMENT_DECLARATION);
    }

    /**
     * Tests that resource requirements can be fulfilled with resource that are registered after the
     * requirement declaration.
     */
    @Test
    public void testRequirementDeclarationWithPendingResource() throws Exception {
        testRequirementDeclaration(
                RequirementDeclarationScenario
                        .TASK_EXECUTOR_REGISTRATION_AFTER_REQUIREMENT_DECLARATION);
    }

    private enum RequirementDeclarationScenario {
        // Tests that a slot request which can be fulfilled will trigger a slot allocation
        TASK_EXECUTOR_REGISTRATION_BEFORE_REQUIREMENT_DECLARATION,
        // Tests that pending slot requests are tried to be fulfilled upon new slot registrations
        TASK_EXECUTOR_REGISTRATION_AFTER_REQUIREMENT_DECLARATION
    }

    private void testRequirementDeclaration(RequirementDeclarationScenario scenario)
            throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceID resourceID = ResourceID.generate();
        final JobID jobId = new JobID();
        final SlotID slotId = SlotID.getDynamicSlotID(resourceID);
        final String targetAddress = "localhost";
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

        final CompletableFuture<
                        Tuple6<
                                SlotID,
                                JobID,
                                AllocationID,
                                ResourceProfile,
                                String,
                                ResourceManagerId>>
                requestFuture = new CompletableFuture<>();
        // accept an incoming slot request
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestFuture.complete(tuple6);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);

        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec(
                                resourceManagerId, new TestingResourceActionsBuilder().build())) {

            if (scenario
                    == RequirementDeclarationScenario
                            .TASK_EXECUTOR_REGISTRATION_BEFORE_REQUIREMENT_DECLARATION) {
                slotManager.registerTaskManager(
                        taskExecutorConnection,
                        new SlotReport(),
                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                        DEFAULT_SLOT_RESOURCE_PROFILE);
            }

            final ResourceRequirements requirements =
                    ResourceRequirements.create(
                            jobId,
                            targetAddress,
                            Collections.singleton(ResourceRequirement.create(resourceProfile, 1)));
            slotManager.processResourceRequirements(requirements);

            if (scenario
                    == RequirementDeclarationScenario
                            .TASK_EXECUTOR_REGISTRATION_AFTER_REQUIREMENT_DECLARATION) {
                slotManager.registerTaskManager(
                        taskExecutorConnection,
                        new SlotReport(),
                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                        DEFAULT_SLOT_RESOURCE_PROFILE);
            }

            assertThat(
                    requestFuture.get(),
                    is(
                            equalTo(
                                    Tuple6.of(
                                            slotId,
                                            jobId,
                                            requestFuture.get().f2,
                                            resourceProfile,
                                            targetAddress,
                                            resourceManagerId))));

            final TaskManagerSlotInformation slot =
                    tracker.getAllocatedOrPendingSlot(requestFuture.get().f2).get();

            assertEquals(
                    "The slot has not been allocated to the expected allocation id.",
                    requestFuture.get().f2,
                    slot.getAllocationId());
        }
    }

    /**
     * Tests that freeing a slot will correctly reset the slot and give back the resource of it to
     * task manager.
     */
    @Test
    public void testFreeSlot() throws Exception {
        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);
        final AllocationID allocationId = new AllocationID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

        final SlotReport slotReport =
                new SlotReport(createAllocatedSlotStatus(allocationId, resourceProfile));

        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec()) {

            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, resourceProfile, resourceProfile);

            final TaskManagerSlotInformation slot =
                    tracker.getAllocatedOrPendingSlot(allocationId).get();

            assertSame(SlotState.ALLOCATED, slot.getState());
            assertEquals(
                    ResourceProfile.ZERO,
                    tracker.getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                            .get()
                            .getAvailableResource());

            slotManager.freeSlot(new SlotID(resourceId, 0), allocationId);

            assertFalse(tracker.getAllocatedOrPendingSlot(allocationId).isPresent());

            assertEquals(
                    resourceProfile,
                    tracker.getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                            .get()
                            .getAvailableResource());
        }
    }

    /**
     * Tests that duplicate resource requirement declaration do not result in additional slots being
     * allocated after a pending slot request has been fulfilled but not yet freed.
     */
    @Test
    public void testDuplicateResourceRequirementDeclarationAfterSuccessfulAllocation()
            throws Exception {
        final CompletableFuture<AllocationID> allocationId = new CompletableFuture<>();
        final AtomicInteger allocateResourceCalls = new AtomicInteger(0);
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceConsumer(
                                tuple6 -> allocateResourceCalls.incrementAndGet())
                        .build();
        final ResourceRequirements requirements = createResourceRequirementsForSingleSlot();

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    allocationId.complete(tuple6.f2);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        final ResourceID resourceId = ResourceID.generate();

        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);
        final SlotReport slotReport = new SlotReport();

        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), resourceManagerActions)) {

            slotManager.registerTaskManager(
                    taskManagerConnection,
                    slotReport,
                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                    DEFAULT_SLOT_RESOURCE_PROFILE);

            slotManager.processResourceRequirements(requirements);

            final TaskManagerSlotInformation slot =
                    tracker.getAllocatedOrPendingSlot(allocationId.get()).get();

            assertThat(slot.getState(), is(SlotState.ALLOCATED));

            slotManager.processResourceRequirements(requirements);
        }

        // check that we have only called the resource allocation only for the first slot request,
        // since the second request is a duplicate
        assertThat(allocateResourceCalls.get(), is(0));
    }

    /**
     * Tests that a resource allocated for one job can be allocated for another job after being
     * freed.
     */
    @Test
    public void testResourceCanBeAllocatedForDifferentJobAfterFree() throws Exception {
        testResourceCanBeAllocatedForDifferentJobAfterFree(
                SecondRequirementDeclarationTime.BEFORE_FREE);
        testResourceCanBeAllocatedForDifferentJobAfterFree(
                SecondRequirementDeclarationTime.AFTER_FREE);
    }

    private enum SecondRequirementDeclarationTime {
        BEFORE_FREE,
        AFTER_FREE
    }

    private void testResourceCanBeAllocatedForDifferentJobAfterFree(
            SecondRequirementDeclarationTime secondRequirementDeclarationTime) throws Exception {
        final CompletableFuture<AllocationID> allocationId1 = new CompletableFuture<>();
        final CompletableFuture<AllocationID> allocationId2 = new CompletableFuture<>();
        final ResourceRequirements resourceRequirements1 =
                createResourceRequirementsForSingleSlot();
        final ResourceRequirements resourceRequirements2 =
                createResourceRequirementsForSingleSlot();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    if (!allocationId1.isDone()) {
                                        allocationId1.complete(tuple6.f2);
                                    } else {
                                        allocationId2.complete(tuple6.f2);
                                    }
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final ResourceID resourceID = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);
        final SlotReport slotReport = new SlotReport();

        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec()) {

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, resourceProfile, resourceProfile);

            slotManager.processResourceRequirements(resourceRequirements1);

            TaskManagerSlotInformation slot =
                    tracker.getAllocatedOrPendingSlot(allocationId1.get()).get();

            assertEquals(
                    "The slot has not been allocated to the expected job id.",
                    resourceRequirements1.getJobId(),
                    slot.getJobId());

            if (secondRequirementDeclarationTime == SecondRequirementDeclarationTime.BEFORE_FREE) {
                slotManager.processResourceRequirements(resourceRequirements2);
            }

            // clear resource requirements first so that the freed slot isn't immediately
            // re-assigned to the job
            slotManager.processResourceRequirements(
                    ResourceRequirements.create(
                            resourceRequirements1.getJobId(),
                            resourceRequirements1.getTargetAddress(),
                            Collections.emptyList()));
            slotManager.freeSlot(SlotID.getDynamicSlotID(resourceID), allocationId1.get());

            if (secondRequirementDeclarationTime == SecondRequirementDeclarationTime.AFTER_FREE) {
                slotManager.processResourceRequirements(resourceRequirements2);
            }

            slot = tracker.getAllocatedOrPendingSlot(allocationId2.get()).get();
            assertEquals(
                    "The slot has not been allocated to the expected job id.",
                    resourceRequirements2.getJobId(),
                    slot.getJobId());
        }
    }

    /**
     * Tests that the slot manager ignores slot reports of unknown origin (not registered task
     * managers).
     */
    @Test
    public void testReceivingUnknownSlotReport() throws Exception {
        final InstanceID unknownInstanceID = new InstanceID();
        final SlotReport unknownSlotReport = new SlotReport();

        try (SlotManager slotManager =
                createFineGrainedSlotManagerBuilder().buildAndStartWithDirectExec()) {
            // check that we don't have any slots registered
            assertThat(slotManager.getNumberRegisteredSlots(), is(0));

            // this should not update anything since the instance id is not known to the slot
            // manager
            assertFalse(slotManager.reportSlotStatus(unknownInstanceID, unknownSlotReport));

            assertThat(slotManager.getNumberRegisteredSlots(), is(0));
        }
    }

    /**
     * Tests that slots are updated with respect to the latest incoming slot report. This means that
     * slots for which a report was received are updated accordingly.
     */
    @Test
    public void testUpdateSlotReport() throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final ResourceID resourceID = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

        final AllocationID allocationId = new AllocationID();

        final SlotStatus slotStatus = createAllocatedSlotStatus(allocationId, resourceProfile);
        final JobID jobId = slotStatus.getJobID();

        final SlotReport slotReport1 = new SlotReport();
        final SlotReport slotReport2 = new SlotReport(slotStatus);

        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec()) {

            // check that we don't have any slots registered
            assertEquals(0, slotManager.getNumberRegisteredSlots());

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport1, resourceProfile, resourceProfile);

            assertEquals(1, slotManager.getNumberRegisteredSlots());

            assertThat(slotManager.getFreeResource(), equalTo(resourceProfile));

            assertTrue(
                    slotManager.reportSlotStatus(
                            taskManagerConnection.getInstanceID(), slotReport2));

            final TaskManagerSlotInformation slot =
                    tracker.getAllocatedOrPendingSlot(allocationId).get();
            assertSame(SlotState.ALLOCATED, slot.getState());
            assertEquals(jobId, slot.getJobId());
        }
    }

    /** Tests that if a slot allocation times out we try to allocate another slot. */
    @Test
    public void testSlotAllocationTimeout() throws Exception {
        final CompletableFuture<Void> secondSlotRequestFuture = new CompletableFuture<>();

        final BlockingQueue<Supplier<CompletableFuture<Acknowledge>>> responseQueue =
                new ArrayBlockingQueue<>(2);
        responseQueue.add(
                () -> FutureUtils.completedExceptionally(new AskTimeoutException("timeout")));
        responseQueue.add(
                () -> {
                    secondSlotRequestFuture.complete(null);
                    return new CompletableFuture<>();
                });

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(ignored -> responseQueue.remove().get())
                        .createTestingTaskExecutorGateway();
        final ResourceID resourceID = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

        final SlotReport slotReport = new SlotReport();

        final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

        try (FineGrainedSlotManager slotManager = createFineGrainedSlotManagerBuilder().build()) {
            slotManager.start(
                    ResourceManagerId.generate(),
                    mainThreadExecutor,
                    new TestingResourceActionsBuilder().build());

            CompletableFuture.runAsync(
                            () ->
                                    slotManager.registerTaskManager(
                                            taskManagerConnection,
                                            slotReport,
                                            resourceProfile.multiply(2),
                                            resourceProfile),
                            mainThreadExecutor)
                    .thenRun(
                            () ->
                                    slotManager.processResourceRequirements(
                                            createResourceRequirementsForSingleSlot()))
                    .get(5, TimeUnit.SECONDS);

            // a second request is only sent if the first request timed out
            secondSlotRequestFuture.get();
        }
    }

    /** Tests that a slot allocation is retried if it times out on the task manager side. */
    @Test
    public void testTaskExecutorSlotAllocationTimeoutHandling() throws Exception {
        final JobID jobId = new JobID();
        final ResourceRequirements resourceRequirements =
                createResourceRequirementsForSingleSlot(jobId);
        final CompletableFuture<Acknowledge> slotRequestFuture1 = new CompletableFuture<>();
        final CompletableFuture<Acknowledge> slotRequestFuture2 = new CompletableFuture<>();
        final Iterator<CompletableFuture<Acknowledge>> slotRequestFutureIterator =
                Arrays.asList(slotRequestFuture1, slotRequestFuture2).iterator();
        final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(2);
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                FunctionUtils.uncheckedFunction(
                                        requestSlotParameters -> {
                                            allocationIds.put(requestSlotParameters.f2);
                                            return slotRequestFutureIterator.next();
                                        }))
                        .createTestingTaskExecutorGateway();

        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);
        final SlotReport slotReport = new SlotReport();

        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec()) {

            slotManager.registerTaskManager(
                    taskManagerConnection,
                    slotReport,
                    resourceProfile.multiply(2),
                    resourceProfile);

            slotManager.processResourceRequirements(resourceRequirements);

            final AllocationID firstAllocationId = allocationIds.take();
            assertThat(allocationIds, is(empty()));

            // let the first attempt fail --> this should trigger a second attempt
            slotRequestFuture1.completeExceptionally(
                    new SlotAllocationException("Test exception."));

            assertThat(getTotalResourceCount(resourceTracker.getAcquiredResources(jobId)), is(1));

            // the second attempt succeeds
            slotRequestFuture2.complete(Acknowledge.get());

            final AllocationID secondAllocationId = allocationIds.take();
            assertThat(allocationIds, is(empty()));

            final TaskManagerSlotInformation slot =
                    tracker.getAllocatedOrPendingSlot(secondAllocationId).get();

            assertThat(slot.getState(), is(SlotState.ALLOCATED));
            assertEquals(jobId, slot.getJobId());

            assertFalse(tracker.getAllocatedOrPendingSlot(firstAllocationId).isPresent());
        }
    }

    /**
     * Tests that free slots which are reported as allocated won't be considered for fulfilling
     * other pending slot requests.
     *
     * <p>See: FLINK-8505
     */
    @Test
    public void testReportAllocatedSlot() throws Exception {
        final ResourceID taskManagerId = ResourceID.generate();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(taskManagerId, taskExecutorGateway);
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();
        final AllocationID allocationId = new AllocationID();
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec()) {

            // initially report a single slot as free
            final SlotReport initialSlotReport = new SlotReport();

            slotManager.registerTaskManager(
                    taskExecutorConnection, initialSlotReport, resourceProfile, resourceProfile);

            assertThat(slotManager.getNumberRegisteredSlots(), is(equalTo(1)));

            // Now report this slot as allocated
            final SlotStatus slotStatus = createAllocatedSlotStatus(allocationId, resourceProfile);
            final SlotReport slotReport = new SlotReport(slotStatus);

            slotManager.reportSlotStatus(taskExecutorConnection.getInstanceID(), slotReport);

            final JobID jobId = new JobID();
            // this resource requirement should not be fulfilled
            final ResourceRequirements requirements =
                    createResourceRequirementsForSingleSlot(jobId);

            slotManager.processResourceRequirements(requirements);

            assertThat(
                    tracker.getAllocatedOrPendingSlot(allocationId).get().getJobId(),
                    is(slotStatus.getJobID()));
            assertThat(
                    getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)), is(1));
        }
    }

    /**
     * Tests that the SlotManager retries allocating a slot if the TaskExecutor#requestSlot call
     * fails.
     */
    @Test
    public void testSlotRequestFailure() throws Exception {
        final TaskManagerTracker tracker = new FineGrainedTaskManagerTracker();
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTracker(tracker)
                        .buildAndStartWithDirectExec()) {

            final ResourceRequirements requirements = createResourceRequirementsForSingleSlot();
            slotManager.processResourceRequirements(requirements);

            final BlockingQueue<
                            Tuple6<
                                    SlotID,
                                    JobID,
                                    AllocationID,
                                    ResourceProfile,
                                    String,
                                    ResourceManagerId>>
                    requestSlotQueue = new ArrayBlockingQueue<>(1);
            final BlockingQueue<CompletableFuture<Acknowledge>> responseQueue =
                    new ArrayBlockingQueue<>(2);

            final CompletableFuture<Acknowledge> firstManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(firstManualSlotRequestResponse);
            final CompletableFuture<Acknowledge> secondManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(secondManualSlotRequestResponse);

            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setRequestSlotFunction(
                                    slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
                                        requestSlotQueue.offer(
                                                slotIDJobIDAllocationIDStringResourceManagerIdTuple6);
                                        try {
                                            return responseQueue.take();
                                        } catch (InterruptedException ignored) {
                                            return FutureUtils.completedExceptionally(
                                                    new FlinkException(
                                                            "Response queue was interrupted."));
                                        }
                                    })
                            .createTestingTaskExecutorGateway();

            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(ResourceID.generate(), testingTaskExecutorGateway);
            final SlotReport slotReport = new SlotReport();

            slotManager.registerTaskManager(
                    taskExecutionConnection,
                    slotReport,
                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                    DEFAULT_SLOT_RESOURCE_PROFILE);

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    firstRequest = requestSlotQueue.take();

            // fail first request
            firstManualSlotRequestResponse.completeExceptionally(
                    new SlotAllocationException("Test exception"));

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    secondRequest = requestSlotQueue.take();

            assertThat(secondRequest.f1, equalTo(firstRequest.f1));
            assertThat(secondRequest.f3, equalTo(firstRequest.f3));

            secondManualSlotRequestResponse.complete(Acknowledge.get());

            final TaskManagerSlotInformation slot =
                    tracker.getAllocatedOrPendingSlot(secondRequest.f2).get();
            assertThat(slot.getState(), equalTo(SlotState.ALLOCATED));
            assertThat(slot.getJobId(), equalTo(secondRequest.f1));
        }
    }

    @Test
    public void testTaskExecutorFailedHandling() throws Exception {
        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .buildAndStartWithDirectExec()) {

            final JobID jobId = new JobID();
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));

            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
            final TaskExecutorConnection taskExecutionConnection1 =
                    new TaskExecutorConnection(ResourceID.generate(), testingTaskExecutorGateway);
            final SlotReport slotReport1 = new SlotReport();

            slotManager.registerTaskManager(
                    taskExecutionConnection1,
                    slotReport1,
                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                    DEFAULT_SLOT_RESOURCE_PROFILE);
            assertThat(
                    getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)), is(0));

            slotManager.unregisterTaskManager(
                    taskExecutionConnection1.getInstanceID(), TEST_EXCEPTION);
            assertThat(
                    getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)), is(1));
        }
    }

    /**
     * Tests that we only request new resources/containers once we have assigned all pending task
     * managers.
     */
    @Test
    public void testRequestNewResources() throws Exception {
        final int numberSlots = 2;
        final AtomicInteger resourceRequests = new AtomicInteger(0);
        final TestingResourceActions testingResourceActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(
                                ignored -> {
                                    resourceRequests.incrementAndGet();
                                    return true;
                                })
                        .build();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setNumSlotsPerWorker(numberSlots)
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), testingResourceActions)) {

            final JobID jobId = new JobID();

            // the first 2 requirements should be fulfillable with the pending task managers of the
            // first allocation (2 slots per worker)
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));
            assertThat(resourceRequests.get(), is(1));

            slotManager.processResourceRequirements(createResourceRequirements(jobId, 2));
            assertThat(resourceRequests.get(), is(1));

            slotManager.processResourceRequirements(createResourceRequirements(jobId, 3));
            assertThat(resourceRequests.get(), is(2));
        }
    }

    @Test
    public void testNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(false);
    }

    @Test
    public void testGracePeriodForNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(true);
    }

    private static void testNotificationAboutNotEnoughResources(boolean withNotificationGracePeriod)
            throws Exception {
        final JobID jobId = new JobID();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final int numRequiredSlots = 3;
        final int numExistingSlots = 1;

        final List<Tuple2<JobID, Collection<ResourceRequirement>>> notEnoughResourceNotifications =
                new ArrayList<>();
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(ignored -> false)
                        .setNotEnoughResourcesConsumer(
                                (jobId1, acquiredResources) ->
                                        notEnoughResourceNotifications.add(
                                                Tuple2.of(jobId1, acquiredResources)))
                        .build();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), resourceManagerActions)) {

            if (withNotificationGracePeriod) {
                // this should disable notifications
                slotManager.setFailUnfulfillableRequest(false);
            }

            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(
                            ResourceID.generate(),
                            new TestingTaskExecutorGatewayBuilder()
                                    .createTestingTaskExecutorGateway());
            final SlotReport slotReport = new SlotReport();
            slotManager.registerTaskManager(
                    taskExecutionConnection,
                    slotReport,
                    resourceProfile.multiply(numExistingSlots),
                    resourceProfile);

            final ResourceRequirements resourceRequirements =
                    createResourceRequirements(jobId, numRequiredSlots);
            slotManager.processResourceRequirements(resourceRequirements);

            if (withNotificationGracePeriod) {
                assertThat(notEnoughResourceNotifications, empty());

                // re-enable notifications which should also trigger another resource check
                slotManager.setFailUnfulfillableRequest(true);
            }

            assertThat(notEnoughResourceNotifications, hasSize(1));
            final Tuple2<JobID, Collection<ResourceRequirement>> notification =
                    notEnoughResourceNotifications.get(0);
            assertThat(notification.f0, is(jobId));
            assertThat(
                    notification.f1,
                    hasItem(ResourceRequirement.create(resourceProfile, numExistingSlots)));
        }
    }

    @Test
    public void testAllocationUpdatesIgnoredIfTaskExecutorUnregistered() throws Exception {
        final ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();

        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        // it is important that the returned future is already completed
                        // otherwise it will be cancelled when the task executor is unregistered
                        .setRequestSlotFunction(
                                ignored -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .createTestingTaskExecutorGateway();

        final SystemExitTrackingSecurityManager trackingSecurityManager =
                new SystemExitTrackingSecurityManager();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        System.setSecurityManager(trackingSecurityManager);
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .setExecutor(executor)
                        .buildAndStartWithDirectExec()) {

            final JobID jobId = new JobID();
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));

            final ResourceID taskExecutorResourceId = ResourceID.generate();
            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(taskExecutorResourceId, taskExecutorGateway);
            final SlotReport slotReport = new SlotReport();

            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, resourceProfile, resourceProfile);
            slotManager.unregisterTaskManager(
                    taskExecutionConnection.getInstanceID(), TEST_EXCEPTION);

            executor.triggerAll();

            assertThat(trackingSecurityManager.getSystemExitFuture().isDone(), is(false));
        } finally {
            System.setSecurityManager(null);
        }
    }

    @Test
    public void testAllocationUpdatesIgnoredIfSlotMarkedAsAllocatedAfterSlotReport()
            throws Exception {
        final ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();

        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        final CompletableFuture<AllocationID> allocationId = new CompletableFuture<>();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        // it is important that the returned future is already completed
                        // otherwise it will be cancelled when the task executor is unregistered
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    allocationId.complete(tuple6.f2);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        final SystemExitTrackingSecurityManager trackingSecurityManager =
                new SystemExitTrackingSecurityManager();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        System.setSecurityManager(trackingSecurityManager);
        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .setExecutor(executor)
                        .buildAndStartWithDirectExec()) {

            slotManager.processResourceRequirements(createResourceRequirements(new JobID(), 1));

            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
            final SlotReport slotReport = new SlotReport();

            slotManager.registerTaskManager(
                    taskExecutionConnection,
                    slotReport,
                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                    DEFAULT_SLOT_RESOURCE_PROFILE);
            slotManager.reportSlotStatus(
                    taskExecutionConnection.getInstanceID(),
                    new SlotReport(createAllocatedSlotStatus(allocationId.get(), resourceProfile)));

            executor.triggerAll();

            assertThat(trackingSecurityManager.getSystemExitFuture().isDone(), is(false));
        } finally {
            System.setSecurityManager(null);
        }
    }

    /**
     * Tests that a task manager timeout does not remove the slots from the SlotManager. A timeout
     * should only trigger the {@link ResourceActions#releaseResource(InstanceID, Exception)}
     * callback. The receiver of the callback can then decide what to do with the TaskManager.
     *
     * <p>See FLINK-7793
     */
    @Test
    public void testTaskManagerTimeoutDoesNotRemoveSlots() throws Exception {
        final Time taskManagerTimeout = Time.milliseconds(10L);

        final CompletableFuture<InstanceID> releaseResourceFuture = new CompletableFuture<>();
        final ResourceActions resourceActions =
                new TestingResourceActionsBuilder()
                        .setReleaseResourceConsumer(
                                (instanceId, ignored) -> releaseResourceFuture.complete(instanceId))
                        .build();
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTimeout(taskManagerTimeout)
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), resourceActions)) {

            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(
                            ResourceID.generate(),
                            new TestingTaskExecutorGatewayBuilder()
                                    .createTestingTaskExecutorGateway());
            slotManager.registerTaskManager(
                    taskExecutionConnection, new SlotReport(), resourceProfile, resourceProfile);
            final InstanceID newTaskExecutorId = taskExecutionConnection.getInstanceID();
            assertEquals(1, slotManager.getNumberRegisteredSlots());
            // wait for the timeout to occur
            assertThat(releaseResourceFuture.get(), is(newTaskExecutorId));
            assertEquals(1, slotManager.getNumberRegisteredSlots());

            slotManager.unregisterTaskManager(newTaskExecutorId, TEST_EXCEPTION);
            assertEquals(0, slotManager.getNumberRegisteredSlots());
        }
    }

    /**
     * Tests that formerly used task managers can timeout after all of their slots have been freed.
     */
    @Test
    public void testTimeoutForUnusedTaskManager() throws Exception {
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);
        final Time taskManagerTimeout = Time.milliseconds(50L);

        final CompletableFuture<InstanceID> releaseResourceFuture = new CompletableFuture<>();
        final AllocationID allocationId = new AllocationID();
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setReleaseResourceConsumer(
                                (instanceID, e) -> releaseResourceFuture.complete(instanceID))
                        .build();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutionConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        final InstanceID instanceId = taskExecutionConnection.getInstanceID();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .setTaskManagerTimeout(taskManagerTimeout)
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), resourceManagerActions)) {

            slotManager.registerTaskManager(
                    taskExecutionConnection,
                    new SlotReport(createAllocatedSlotStatus(allocationId, resourceProfile)),
                    resourceProfile,
                    resourceProfile);

            assertEquals(slotManager.getTaskManagerIdleSince(instanceId), Long.MAX_VALUE);

            slotManager.freeSlot(
                    new SlotID(taskExecutionConnection.getResourceID(), 0), allocationId);

            assertThat(
                    slotManager.getTaskManagerIdleSince(instanceId), not(equalTo(Long.MAX_VALUE)));
            assertThat(releaseResourceFuture.get(), is(equalTo(instanceId)));
        }
    }

    /**
     * Test that the slot manager only allocates new workers if their worker spec can fulfill the
     * requested resource profile.
     */
    @Test
    public void testWorkerOnlyAllocatedIfRequestedSlotCouldBeFulfilled() throws Exception {
        final ResourceProfile largeResourceProfile = ResourceProfile.fromResources(1000, 1337);

        final AtomicInteger resourceRequests = new AtomicInteger(0);
        final ResourceActions resourceActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(
                                ignored -> {
                                    resourceRequests.incrementAndGet();
                                    return true;
                                })
                        .build();

        try (FineGrainedSlotManager slotManager =
                createFineGrainedSlotManagerBuilder()
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), resourceActions)) {

            slotManager.processResourceRequirements(
                    createResourceRequirements(new JobID(), 1, largeResourceProfile));
            assertThat(resourceRequests.get(), is(0));
        }
    }

    private static ResourceRequirements createResourceRequirementsForSingleSlot() {
        return createResourceRequirementsForSingleSlot(new JobID());
    }

    private static ResourceRequirements createResourceRequirementsForSingleSlot(JobID jobId) {
        return createResourceRequirements(jobId, 1);
    }

    private static ResourceRequirements createResourceRequirements(
            JobID jobId, int numRequiredSlots) {
        return createResourceRequirements(jobId, numRequiredSlots, ResourceProfile.UNKNOWN);
    }

    private static ResourceRequirements createResourceRequirements(
            JobID jobId, int numRequiredSlots, ResourceProfile resourceProfile) {
        return ResourceRequirements.create(
                jobId,
                "foobar",
                Collections.singleton(
                        ResourceRequirement.create(resourceProfile, numRequiredSlots)));
    }

    private static FineGrainedSlotManagerBuilder createFineGrainedSlotManagerBuilder() {
        return FineGrainedSlotManagerBuilder.newBuilder()
                .setDefaultWorkerResourceSpec(DEFAULT_WORKER_RESOURCE_SPEC)
                .setNumSlotsPerWorker(DEFAULT_NUM_SLOTS_PER_WORKER);
    }

    private static SlotStatus createAllocatedSlotStatus(
            AllocationID allocationID, ResourceProfile resourceProfile) {
        return new SlotStatus(
                new SlotID(ResourceID.generate(), 0), resourceProfile, new JobID(), allocationID);
    }

    private static int getTotalResourceCount(Collection<ResourceRequirement> resources) {
        if (resources == null) {
            return 0;
        }
        return resources.stream()
                .map(ResourceRequirement::getNumberOfRequiredSlots)
                .reduce(0, Integer::sum);
    }
}
