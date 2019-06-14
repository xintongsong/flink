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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the {@link StandaloneResourceManagerTest}.
 */
public class StandaloneResourceManagerTest extends TestLogger {

	private static final Time RPC_TIMEOUT = Time.seconds(10L);

	private static final Time STANDALONE_SLOT_RESOURCES_REGISTRATION_TIMEOUT = Time.seconds(1L);

	private static final long HEARTBEAT_TIMEOUT_IN_MILLS = 5000;

	private static TestingRpcService rpcService;

	private ResourceID jobMasterResourceID;

	private ResourceID taskExecutorResourceID;

	private JobMasterGateway jobMasterGateway;

	private JobID jobID;

	private SettableLeaderRetrievalService jobMasterLeaderRetrievalService;

	private TestingTaskExecutorGateway taskExecutorGateway;

	private int dataPort = 1234;

	private HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

	private TestingLeaderElectionService resourceManagerLeaderElectionService;

	private TestingHighAvailabilityServices haServices;

	private ResourceManager resourceManager;

	private ResourceManagerGateway rmGateway;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@Before
	public void setup() throws Exception {
		rpcService = new TestingRpcService();
		jobID = new JobID();

		createAndRegisterJobMasterGateway();
		jobMasterResourceID = ResourceID.generate();

		jobMasterLeaderRetrievalService = new SettableLeaderRetrievalService(
			jobMasterGateway.getAddress(),
			jobMasterGateway.getFencingToken().toUUID());
		resourceManagerLeaderElectionService = new TestingLeaderElectionService();

		haServices = new TestingHighAvailabilityServicesBuilder()
			.setJobMasterLeaderRetrieverFunction(requestedJobId -> {
				if (requestedJobId.equals(jobID)) {
					return jobMasterLeaderRetrievalService;
				} else {
					throw new FlinkRuntimeException(String.format("Unknown job id %s", jobID));
				}
			})
			.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService)
			.build();

		testingFatalErrorHandler = new TestingFatalErrorHandler();
		resourceManager = createAndStartResourceManager();
		resourceManagerLeaderElectionService.isLeader(UUID.randomUUID()).get();
		rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

		createAndRegisterTaskExecutorGateway();
		taskExecutorResourceID = ResourceID.generate();
	}

	private void createAndRegisterJobMasterGateway() {
		jobMasterGateway = spy(new TestingJobMasterGatewayBuilder().build());
		rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
	}

	private void createAndRegisterTaskExecutorGateway() {
		taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);
	}

	private ResourceManager<?> createAndStartResourceManager() throws Exception {
		ResourceID rmResourceId = ResourceID.generate();

		HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, HEARTBEAT_TIMEOUT_IN_MILLS);

		JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			haServices,
			rpcService.getScheduledExecutor(),
			Time.minutes(5L));

		final SlotManager slotManager = SlotManagerBuilder.newBuilder()
			.setScheduledExecutor(rpcService.getScheduledExecutor())
			.build();

		ResourceManager<?> resourceManager = new StandaloneResourceManager(
			rpcService,
			ResourceManager.RESOURCE_MANAGER_NAME,
			rmResourceId,
			haServices,
			heartbeatServices,
			slotManager,
			NoOpMetricRegistry.INSTANCE,
			jobLeaderIdService,
			new ClusterInformation("localhost", 1234),
			testingFatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			STANDALONE_SLOT_RESOURCES_REGISTRATION_TIMEOUT);

		resourceManager.start();

		return resourceManager;
	}

	@After
	public void teardown() throws Exception {
		if (resourceManager != null) {
			RpcUtils.terminateRpcEndpoint(resourceManager, RPC_TIMEOUT);
		}

		if (testingFatalErrorHandler != null && testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	@AfterClass
	public static void teardownClass() throws Exception {
		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, RPC_TIMEOUT);
		}
	}

	/**
	 * Tests that standalone resource manager set allowed slot resource profiles based on registered slots to
	 * slot manager after cluster initialization time, and slot managers fail slot requests that can not be satisfied
	 * properly.
	 */
	@Test
	public void testSetAllowedSlotResourceProfiles() throws Exception {

		// register job manager

		rmGateway.registerJobManager(
			jobMasterGateway.getFencingToken(),
			jobMasterResourceID,
			jobMasterGateway.getAddress(),
			jobID,
			RPC_TIMEOUT).get();

		// request slots before TM registered
		// both requests should receive ack, because allowed slot resource profiles are not set yet

		SlotRequest slotRequest1 = new SlotRequest(
			jobID,
			new AllocationID(),
			new ResourceProfile(1.0, 1000),
			jobMasterGateway.getAddress());
		SlotRequest slotRequest2 = new SlotRequest(
			jobID,
			new AllocationID(),
			new ResourceProfile(1.0, 500),
			jobMasterGateway.getAddress());

		rmGateway.requestSlot(
			jobMasterGateway.getFencingToken(),
			slotRequest1,
			RPC_TIMEOUT).get();
		rmGateway.requestSlot(
			jobMasterGateway.getFencingToken(),
			slotRequest2,
			RPC_TIMEOUT).get();

		// register task manager
		// request 1 should be notified failure

		RegistrationResponse tmRegistrationResponse = rmGateway.registerTaskExecutor(
			taskExecutorGateway.getAddress(),
			taskExecutorResourceID,
			dataPort,
			hardwareDescription,
			RPC_TIMEOUT).get();

		SlotReport slotReport = new SlotReport(Collections.singleton(new SlotStatus(
			new SlotID(ResourceID.generate(), 0),
			new ResourceProfile(1.0, 500))));
		rmGateway.sendSlotReport(
			taskExecutorResourceID,
			((TaskExecutorRegistrationSuccess) tmRegistrationResponse).getRegistrationId(),
			slotReport,
			RPC_TIMEOUT).get();

		Thread.sleep(2 * STANDALONE_SLOT_RESOURCES_REGISTRATION_TIMEOUT.toMilliseconds());

		verify(jobMasterGateway, times(1)).notifyAllocationFailure(
			Mockito.eq(slotRequest1.getAllocationId()), Mockito.any(Exception.class));
		verify(jobMasterGateway, times(0)).notifyAllocationFailure(
			Mockito.eq(slotRequest2.getAllocationId()), Mockito.any(Exception.class));

		// request slots after TM registered
		// request 3 should fail, request 4 should receive ack

		SlotRequest slotRequest3 = new SlotRequest(
			jobID,
			new AllocationID(),
			new ResourceProfile(1.0, 1000),
			jobMasterGateway.getAddress());
		SlotRequest slotRequest4 = new SlotRequest(
			jobID,
			new AllocationID(),
			new ResourceProfile(1.0, 500),
			jobMasterGateway.getAddress());

		Exception exception = null;
		try {
			rmGateway.requestSlot(
				jobMasterGateway.getFencingToken(),
				slotRequest3,
				RPC_TIMEOUT).get();
		} catch (Exception e) {
			exception = e;
		}
		assertNotNull(exception);

		rmGateway.requestSlot(
			jobMasterGateway.getFencingToken(),
			slotRequest4,
			RPC_TIMEOUT).get();
	}
}
