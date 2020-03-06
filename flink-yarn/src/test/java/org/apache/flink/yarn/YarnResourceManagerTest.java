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

package org.apache.flink.yarn;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_APP_ID;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_HOME_DIR;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_SHIP_FILES;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_HADOOP_USER_NAME;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_JAR_PATH;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_YARN_FILES;
import static org.apache.flink.yarn.YarnResourceManager.ERROR_MASSAGE_ON_SHUTDOWN_REQUEST;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the YARN resource manager component.
 */
public class YarnResourceManagerTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	private Configuration flinkConfig;

	private Map<String, String> env;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		testingFatalErrorHandler = new TestingFatalErrorHandler();

		flinkConfig = new Configuration();
		flinkConfig.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 100);
		flinkConfig.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("1g"));

		File root = folder.getRoot();
		File home = new File(root, "home");
		boolean created = home.mkdir();
		assertTrue(created);

		env = new HashMap<>();
		env.put(ENV_APP_ID, "foo");
		env.put(ENV_CLIENT_HOME_DIR, home.getAbsolutePath());
		env.put(ENV_CLIENT_SHIP_FILES, "");
		env.put(ENV_FLINK_CLASSPATH, "");
		env.put(ENV_HADOOP_USER_NAME, "foo");
		env.put(FLINK_JAR_PATH, root.toURI().toString());
		env.put(ApplicationConstants.Environment.PWD.key(), home.getAbsolutePath());

		BootstrapTools.writeConfiguration(flinkConfig, new File(home.getAbsolutePath(), FLINK_CONF_FILENAME));
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}

		if (env != null) {
			env.clear();
		}
	}

	static class TestingYarnResourceManager extends YarnResourceManager {
		final TestingYarnAMRMClientAsync testingYarnAMRMClientAsync;
		final TestingYarnNMClientAsync testingYarnNMClientAsync;
		@Nullable
		Map<WorkerResourceSpec, Integer> pendingWorkerNums;

		TestingYarnResourceManager(
				RpcService rpcService,
				String resourceManagerEndpointId,
				ResourceID resourceId,
				Configuration flinkConfig,
				Map<String, String> env,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				SlotManager slotManager,
				JobLeaderIdService jobLeaderIdService,
				ClusterInformation clusterInformation,
				FatalErrorHandler fatalErrorHandler,
				@Nullable String webInterfaceUrl,
				ResourceManagerMetricGroup resourceManagerMetricGroup) {
			super(
				rpcService,
				resourceManagerEndpointId,
				resourceId,
				flinkConfig,
				env,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				webInterfaceUrl,
				resourceManagerMetricGroup);
			this.testingYarnNMClientAsync = new TestingYarnNMClientAsync(this);
			this.testingYarnAMRMClientAsync = new TestingYarnAMRMClientAsync(this);
			this.pendingWorkerNums = null;
		}

		<T> CompletableFuture<T> runInMainThread(Callable<T> callable) {
			return callAsync(callable, TIMEOUT);
		}

		MainThreadExecutor getMainThreadExecutorForTesting() {
			return super.getMainThreadExecutor();
		}

		@Override
		protected AMRMClientAsync<AMRMClient.ContainerRequest> createAndStartResourceManagerClient(
				YarnConfiguration yarnConfiguration,
				int yarnHeartbeatIntervalMillis,
				@Nullable String webInterfaceUrl) {
			return testingYarnAMRMClientAsync;
		}

		@Override
		protected NMClientAsync createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
			return testingYarnNMClientAsync;
		}

		@Override
		public Map<WorkerResourceSpec, Integer> getPendingWorkerNums() {
			return pendingWorkerNums != null ? pendingWorkerNums : super.getPendingWorkerNums();
		}
	}

	class Context {

		// services
		final TestingRpcService rpcService;
		final MockResourceManagerRuntimeServices rmServices;

		// RM
		final ResourceID rmResourceID;
		static final String RM_ADDRESS = "resourceManager";
		final TestingYarnResourceManager resourceManager;

		final int dataPort = 1234;
		final HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

		// domain objects for test purposes
		final ResourceProfile resourceProfile1 = ResourceProfile.UNKNOWN;

		final WorkerResourceSpec workerResourceSpec = new WorkerResourceSpec(1.0, 100, 100, 100, 100);

		public String taskHost = "host1";

		final TestingYarnNMClientAsync testingYarnNMClientAsync;

		final TestingYarnAMRMClientAsync testingYarnAMRMClientAsync;

		int containerIdx = 0;

		/**
		 * Create mock RM dependencies.
		 */
		Context() throws Exception {
			this(flinkConfig);
		}

		Context(Configuration configuration) throws  Exception {
			rpcService = new TestingRpcService();
			rmServices = new MockResourceManagerRuntimeServices(rpcService, TIMEOUT);

			// resource manager
			rmResourceID = ResourceID.generate();
			resourceManager =
					new TestingYarnResourceManager(
							rpcService,
							RM_ADDRESS,
							rmResourceID,
							configuration,
							env,
							rmServices.highAvailabilityServices,
							rmServices.heartbeatServices,
							rmServices.slotManager,
							rmServices.jobLeaderIdService,
							new ClusterInformation("localhost", 1234),
							testingFatalErrorHandler,
							null,
							UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());

			testingYarnAMRMClientAsync = resourceManager.testingYarnAMRMClientAsync;
			testingYarnNMClientAsync = resourceManager.testingYarnNMClientAsync;
		}

		/**
		 * Start the resource manager and grant leadership to it.
		 */
		void startResourceManager() throws Exception {
			resourceManager.start();
			rmServices.grantLeadership();
		}

		/**
		 * Stop the Akka actor system.
		 */
		void stopResourceManager() throws Exception {
			rpcService.stopService().get();
		}

		/**
		 * A wrapper function for running test. Deal with setup and teardown logic
		 * in Context.
		 * @param testMethod the real test body.
		 */
		void runTest(RunnableWithException testMethod) throws Exception {
			startResourceManager();
			try {
				testMethod.run();
			} finally {
				stopResourceManager();
			}
		}

		void verifyFutureCompleted(CompletableFuture future) throws Exception {
			future.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		}

		Container createTestingContainer() {
			return createTestingContainerWithResource(resourceManager.getContainerResource(workerResourceSpec));
		}

		Container createTestingContainerWithResource(Resource resource) {
			final ContainerId containerId = ContainerId.newInstance(
				ApplicationAttemptId.newInstance(
					ApplicationId.newInstance(System.currentTimeMillis(), 1),
					1),
				containerIdx++);
			final NodeId nodeId = NodeId.newInstance("container", 1234);
			return new TestingContainer(containerId, nodeId, resource, Priority.UNDEFINED);
		}

		ContainerStatus createTestingContainerStatus(final ContainerId containerId) {
			return new TestingContainerStatus(containerId, ContainerState.COMPLETE, "Test exit", -1);
		}
	}

	@Test
	public void testShutdownRequestCausesFatalError() throws Exception {
		new Context() {{
			runTest(() -> {
				resourceManager.onShutdownRequest();

				Throwable t = testingFatalErrorHandler.getErrorFuture().get(2000L, TimeUnit.MILLISECONDS);
				assertThat(ExceptionUtils.findThrowable(t, ResourceManagerException.class).isPresent(), is(true));
				assertThat(ExceptionUtils.findThrowableWithMessage(t, ERROR_MASSAGE_ON_SHUTDOWN_REQUEST).isPresent(), is(true));

				testingFatalErrorHandler.clearError();
			});
		}};
	}

	@Test
	public void testStopWorker() throws Exception {
		new Context() {{
			final CompletableFuture<Void> addContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> releaseAssignedContainerFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();
			final CompletableFuture<Void> stopContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(workerResourceSpec))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(ignored -> addContainerRequestFuture.complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer(ignored -> removeContainerRequestFuture.complete(null));
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer(ignored -> releaseAssignedContainerFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer(ignored -> startContainerAsyncFuture.complete(null));
			testingYarnNMClientAsync.setStopContainerAsyncConsumer(ignored -> stopContainerAsyncFuture.complete(null));

			runTest(() -> {
				// Request slot from SlotManager.
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = createTestingContainer();

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyFutureCompleted(addContainerRequestFuture);
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				// Remote task executor registers with YarnResourceManager.
				rpcService.registerGateway(taskHost, new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				final ResourceID taskManagerResourceId = new ResourceID(testingContainer.getId().toString());
				final ResourceProfile resourceProfile = ResourceProfile.newBuilder()
					.setCpuCores(10.0)
					.setTaskHeapMemoryMB(1)
					.setTaskOffHeapMemoryMB(1)
					.setManagedMemoryMB(1)
					.setNetworkMemoryMB(0)
					.build();
				final SlotReport slotReport = new SlotReport(
					new SlotStatus(new SlotID(taskManagerResourceId, 1), resourceProfile));

				TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
					taskHost,
					taskManagerResourceId,
					dataPort,
					hardwareDescription,
					ResourceProfile.ZERO,
					ResourceProfile.ZERO);
				CompletableFuture<Integer> numberRegisteredSlotsFuture = rmGateway
					.registerTaskExecutor(taskExecutorRegistration, Time.seconds(10L))
					.thenCompose(
						(RegistrationResponse response) -> {
							assertThat(response, instanceOf(TaskExecutorRegistrationSuccess.class));
							final TaskExecutorRegistrationSuccess success = (TaskExecutorRegistrationSuccess) response;
							return rmGateway.sendSlotReport(
								taskManagerResourceId,
								success.getRegistrationId(),
								slotReport,
								Time.seconds(10L));
						})
					.handleAsync(
						(Acknowledge ignored, Throwable throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
						resourceManager.getMainThreadExecutorForTesting());

				final int numberRegisteredSlots = numberRegisteredSlotsFuture.get();

				assertEquals(1, numberRegisteredSlots);

				// Unregister all task executors and release all containers.
				CompletableFuture<?> unregisterAndReleaseFuture = resourceManager.runInMainThread(() -> {
					rmServices.slotManager.unregisterTaskManagersAndReleaseResources();
					return null;
				});

				unregisterAndReleaseFuture.get();

				verifyFutureCompleted(stopContainerAsyncFuture);
				verifyFutureCompleted(releaseAssignedContainerFuture);
			});

			// It's now safe to access the SlotManager state since the ResourceManager has been stopped.
			assertThat(rmServices.slotManager.getNumberRegisteredSlots(), Matchers.equalTo(0));
			assertThat(resourceManager.getNumberOfRegisteredTaskManagers().get(), Matchers.equalTo(0));
		}};
	}

	/**
	 * Tests that application files are deleted when the YARN application master is de-registered.
	 */
	@Test
	public void testDeleteApplicationFiles() throws Exception {
		new Context() {{
			final File applicationDir = folder.newFolder(".flink");
			env.put(FLINK_YARN_FILES, applicationDir.getCanonicalPath());

			runTest(() -> {
				resourceManager.deregisterApplication(ApplicationStatus.SUCCEEDED, null);
				assertFalse("YARN application directory was not removed", Files.exists(applicationDir.toPath()));
			});
		}};
	}

	/**
	 * Tests that YarnResourceManager will not request more containers than needs during
	 * callback from Yarn when container is Completed.
	 */
	@Test
	public void testOnContainerCompleted() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(workerResourceSpec))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(ignored ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer(ignored -> removeContainerRequestFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer(ignored -> startContainerAsyncFuture.complete(null));

			runTest(() -> {
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = createTestingContainer();

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				// Callback from YARN when container is Completed, pending request can not be fulfilled by pending
				// containers, need to request new container.
				ContainerStatus testingContainerStatus = createTestingContainerStatus(testingContainer.getId());

				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				verifyFutureCompleted(addContainerRequestFutures.get(1));

				// Callback from YARN when container is Completed happened before global fail, pending request
				// slot is already fulfilled by pending containers, no need to request new container.
				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				assertFalse(addContainerRequestFutures.get(2).isDone());
			});
		}};
	}

	@Test
	public void testOnStartContainerError() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> releaseAssignedContainerFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(workerResourceSpec))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(ignored ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer(ignored -> removeContainerRequestFuture.complete(null));
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer(ignored -> releaseAssignedContainerFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer(ignored -> startContainerAsyncFuture.complete(null));

			runTest(() -> {
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				Container testingContainer = createTestingContainer();

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				resourceManager.onStartContainerError(testingContainer.getId(), new Exception("start error"));
				verifyFutureCompleted(releaseAssignedContainerFuture);
				verifyFutureCompleted(addContainerRequestFutures.get(1));
			});
		}};
	}

	@Test
	public void testGetCpuCoresCommonOption() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setDouble(TaskManagerOptions.CPU_CORES, 1.0);
		configuration.setInteger(YarnConfigOptions.VCORES, 2);
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);

		new Context() {{
			runTest(() -> assertThat(resourceManager.getCpuCores(configuration), is(1.0)));
		}};
	}

	@Test
	public void testGetCpuCoresYarnOption() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setInteger(YarnConfigOptions.VCORES, 2);
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);

		new Context() {{
			runTest(() -> assertThat(resourceManager.getCpuCores(configuration), is(2.0)));
		}};
	}

	@Test
	public void testGetCpuCoresNumSlots() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);

		new Context() {{
			runTest(() -> assertThat(resourceManager.getCpuCores(configuration), is(3.0)));
		}};
	}

	@Test
	public void testGetCpuRoundUp() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setDouble(TaskManagerOptions.CPU_CORES, 0.5);

		new Context() {{
			runTest(() -> assertThat(resourceManager.getCpuCores(configuration), is(1.0)));
		}};
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testGetCpuExceedMaxInt() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setDouble(TaskManagerOptions.CPU_CORES, Double.MAX_VALUE);

		new Context() {{
			resourceManager.getCpuCores(configuration);
		}};
	}

	@Test
	public void testWorkerSpecContainerResourceAdapter_MatchVcores() {
		final int minMemMB = 100;
		final int minVcore = 10;
		final YarnResourceManager.WorkerSpecContainerResourceAdapter adapter =
			new YarnResourceManager.WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(), minMemMB, minVcore, true);

		final WorkerResourceSpec workerSpec1 = new WorkerResourceSpec(1.0, 10, 10, 10, 10);
		final WorkerResourceSpec workerSpec2 = new WorkerResourceSpec(10.0, 25, 25, 25, 25);
		final WorkerResourceSpec workerSpec3 = new WorkerResourceSpec(5.0, 30, 30, 30, 30);
		final WorkerResourceSpec workerSpec4 = new WorkerResourceSpec(15.0, 10, 10, 10, 10);

		final Resource containerResource1 = Resource.newInstance(100, 10);
		final Resource containerResource2 = Resource.newInstance(200, 10);
		final Resource containerResource3 = Resource.newInstance(100, 20);

		assertThat(adapter.getWorkerSpecs(containerResource1), empty());
		assertThat(adapter.getWorkerSpecs(containerResource2), empty());

		assertThat(adapter.getContainerResource(workerSpec1), is(containerResource1));
		assertThat(adapter.getContainerResource(workerSpec2), is(containerResource1));
		assertThat(adapter.getContainerResource(workerSpec3), is(containerResource2));
		assertThat(adapter.getContainerResource(workerSpec4), is(containerResource3));

		assertThat(adapter.getWorkerSpecs(containerResource1), hasSize(2));
		assertThat(adapter.getWorkerSpecs(containerResource2), hasSize(1));
		assertThat(adapter.getWorkerSpecs(containerResource3), hasSize(1));
		assertThat(adapter.getWorkerSpecs(containerResource1), containsInAnyOrder(workerSpec1, workerSpec2));
		assertThat(adapter.getWorkerSpecs(containerResource2), contains(workerSpec3));
		assertThat(adapter.getWorkerSpecs(containerResource3), contains(workerSpec4));
	}

	@Test
	public void testWorkerSpecContainerResourceAdapter_NotMatchVcores() {
		final int minMemMB = 100;
		final int minVcore = 1;
		final YarnResourceManager.WorkerSpecContainerResourceAdapter adapter =
			new YarnResourceManager.WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(), minMemMB, minVcore, false);

		final WorkerResourceSpec workerSpec1 = new WorkerResourceSpec(5.0, 10, 10, 10, 10);
		final WorkerResourceSpec workerSpec2 = new WorkerResourceSpec(10.0, 10, 10, 10, 10);
		final WorkerResourceSpec workerSpec3 = new WorkerResourceSpec(5.0, 25, 25, 25, 25);
		final WorkerResourceSpec workerSpec4 = new WorkerResourceSpec(5.0, 30, 30, 30, 30);

		final Resource containerResource1 = Resource.newInstance(100, 5);
		final Resource containerResource2 = Resource.newInstance(100, 10);
		final Resource containerResource3 = Resource.newInstance(200, 5);

		final Resource containerResource4 = Resource.newInstance(100, 1);
		final Resource containerResource5 = Resource.newInstance(200, 1);

		assertThat(adapter.getContainerResource(workerSpec1), is(containerResource1));
		assertThat(adapter.getContainerResource(workerSpec2), is(containerResource2));
		assertThat(adapter.getContainerResource(workerSpec3), is(containerResource1));
		assertThat(adapter.getContainerResource(workerSpec4), is(containerResource3));

		assertThat(adapter.getEquivalentContainerResource(containerResource4), hasSize(2));
		assertThat(adapter.getEquivalentContainerResource(containerResource5), hasSize(1));
		assertThat(adapter.getEquivalentContainerResource(containerResource4),
			containsInAnyOrder(containerResource1, containerResource2));
		assertThat(adapter.getEquivalentContainerResource(containerResource5), contains(containerResource3));

		assertThat(adapter.getWorkerSpecs(containerResource4), hasSize(3));
		assertThat(adapter.getWorkerSpecs(containerResource5), hasSize(1));
		assertThat(adapter.getWorkerSpecs(containerResource4),
			containsInAnyOrder(workerSpec1, workerSpec2, workerSpec3));
		assertThat(adapter.getWorkerSpecs(containerResource5), contains(workerSpec4));
	}

	@Test
	public void testStartWorkerVariousSpec_SameContainerResource() throws Exception{
		new Context() {{
			final WorkerResourceSpec workerResourceSpec1 = new WorkerResourceSpec(1, 100, 100, 100, 100);
			final WorkerResourceSpec workerResourceSpec2 = new WorkerResourceSpec(1, 99, 100, 100, 100);
			final Resource containerResource = resourceManager.getContainerResource(workerResourceSpec1);

			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final String startCommand1 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (100L << 20);
			final String startCommand2 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (99L << 20);
			final CompletableFuture<Void> startContainerAsyncCommandFuture1 = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncCommandFuture2 = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(ImmutableList.of(
					resourceManager.getContainerRequest(workerResourceSpec1),
					resourceManager.getContainerRequest(workerResourceSpec2))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(tuple ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer(tuple -> {
				if (containsStartCommand(tuple.f1, startCommand1)) {
					startContainerAsyncCommandFuture1.complete(null);
				} else if (containsStartCommand(tuple.f1, startCommand2)) {
					startContainerAsyncCommandFuture2.complete(null);
				}
			});

			runTest(() -> {
				// Make sure two worker resource spec will be normalized to the same container resource
				assertEquals(containerResource, resourceManager.getContainerResource(workerResourceSpec2));

				resourceManager.startNewWorker(workerResourceSpec1);
				resourceManager.startNewWorker(workerResourceSpec2);

				// Verify both containers requested
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(addContainerRequestFutures.get(1));

				// Mock that both containers are allocated
				Container container1 = createTestingContainer();
				Container container2 = createTestingContainer();
				resourceManager.onContainersAllocated(ImmutableList.of(container1, container2));

				// Verify workers with both spec are started.
				verifyFutureCompleted(startContainerAsyncCommandFuture1);
				verifyFutureCompleted(startContainerAsyncCommandFuture2);

				// Mock that one container is completed, while the worker is still pending
				resourceManager.pendingWorkerNums = Collections.singletonMap(workerResourceSpec1, 1);
				ContainerStatus testingContainerStatus = createTestingContainerStatus(container1.getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// Verify that only one more container is requested.
				verifyFutureCompleted(addContainerRequestFutures.get(2));
				assertFalse(addContainerRequestFutures.get(3).isDone());
			});
		}};
	}

	@Test
	public void testStartWorkerVariousSpec_DifferentContainerResource() throws Exception{
		new Context() {{
			final WorkerResourceSpec workerResourceSpec1 = new WorkerResourceSpec(1, 100, 100, 100, 100);
			final WorkerResourceSpec workerResourceSpec2 = new WorkerResourceSpec(10, 1000, 1000, 1000, 1000);
			final Resource containerResource1 = resourceManager.getContainerResource(workerResourceSpec1);
			final Resource containerResource2 = resourceManager.getContainerResource(workerResourceSpec2);

			final List<CompletableFuture<Resource>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final String startCommand1 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (100L << 20);
			final String startCommand2 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (99L << 20);
			final CompletableFuture<Void> startContainerAsyncCommandFuture1 = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncCommandFuture2 = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(tuple -> {
				if (tuple.f2.equals(containerResource1)) {
					return Collections.singletonList(
						Collections.singletonList(resourceManager.getContainerRequest(workerResourceSpec1)));
				} else if (tuple.f2.equals(containerResource2)) {
					return Collections.singletonList(
						Collections.singletonList(resourceManager.getContainerRequest(workerResourceSpec2)));
				}
				return null;
			});
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer(tuple ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(tuple.f0.getCapability()));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer(tuple -> {
				if (containsStartCommand(tuple.f1, startCommand1)) {
					startContainerAsyncCommandFuture1.complete(null);
				} else if (containsStartCommand(tuple.f1, startCommand2)) {
					startContainerAsyncCommandFuture2.complete(null);
				}
			});

			runTest(() -> {
				// Make sure two worker resource spec will be normalized to different container resources
				assertNotEquals(containerResource1, containerResource2);

				resourceManager.startNewWorker(workerResourceSpec1);
				resourceManager.startNewWorker(workerResourceSpec2);

				// Verify both containers requested
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(addContainerRequestFutures.get(1));

				// Mock that container 1 is allocated
				Container container1 = createTestingContainerWithResource(containerResource1);
				resourceManager.onContainersAllocated(Collections.singletonList(container1));

				// Verify that only worker with spec1 is started.
				verifyFutureCompleted(startContainerAsyncCommandFuture1);
				assertFalse(startContainerAsyncCommandFuture2.isDone());

				// Mock that container 1 is completed, while the worker is still pending
				resourceManager.pendingWorkerNums = Collections.singletonMap(workerResourceSpec1, 1);
				ContainerStatus testingContainerStatus = createTestingContainerStatus(container1.getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// Verify that only container 1 is requested again
				verifyFutureCompleted(addContainerRequestFutures.get(2));
				assertThat(addContainerRequestFutures.get(2).get(), is(containerResource1));
				assertFalse(addContainerRequestFutures.get(3).isDone());
			});
		}};
	}

	private boolean containsStartCommand(ContainerLaunchContext containerLaunchContext, String command) {
		return containerLaunchContext.getCommands().stream().anyMatch(str -> str.contains(command));
	}

	private Configuration getConfigProcessSpecEqualsWorkerSpec() {
		final Configuration config = new Configuration();
		config.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ZERO);
		config.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.ZERO);
		config.set(TaskManagerOptions.JVM_METASPACE, MemorySize.ZERO);
		config.set(TaskManagerOptions.JVM_OVERHEAD_MIN, MemorySize.ZERO);
		config.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.ZERO);
		return config;
	}

	private void registerSlotRequest(
			TestingYarnResourceManager resourceManager,
			MockResourceManagerRuntimeServices rmServices,
			ResourceProfile resourceProfile,
			String taskHost) throws ExecutionException, InterruptedException {

		CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
			rmServices.slotManager.registerSlotRequest(
				new SlotRequest(new JobID(), new AllocationID(), resourceProfile, taskHost));
			return null;
		});

		// wait for the registerSlotRequest completion
		registerSlotRequestFuture.get();
	}
}
