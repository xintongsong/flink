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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.history.HistoryServerUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * The yarn implementation of the resource manager. Used when the system is started
 * via the resource framework YARN.
 */
public class YarnResourceManager extends ActiveResourceManager<YarnWorkerNode>
		implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

	private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(1);

	/** YARN container map. Package private for unit test purposes. */
	private final ConcurrentMap<ResourceID, YarnWorkerNode> workerNodeMap;
	/** Environment variable name of the final container id used by the YarnResourceManager.
	 * Container ID generation may vary across Hadoop versions. */
	static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	/** Environment variable name of the hostname given by the YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka */
	static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	static final String ERROR_MASSAGE_ON_SHUTDOWN_REQUEST = "Received shutdown request from YARN ResourceManager.";

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	private final YarnConfiguration yarnConfig;

	@Nullable
	private final String webInterfaceUrl;

	/** The heartbeat interval while the resource master is waiting for containers. */
	private final int containerRequestHeartbeatIntervalMillis;

	/** Client to communicate with the Resource Manager (YARN's master). */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskExecutor processes. */
	private NMClientAsync nodeManagerClient;

	/** Map from worker resource to container resource. */
	private final Map<WorkerResourceSpec, Resource> containerResources;

	/** Map from container resource to worker resources. */
	private final Map<Resource, Set<WorkerResourceSpec>> workerResources;

	/** Map from worker resource to number of pending contaienrs. */
	private final Map<WorkerResourceSpec, Integer> pendingWorkerNums;

	private final int yarnMinAllocationMB;

	private final int yarnMinAllocationVcore;

	public YarnResourceManager(
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
			flinkConfig,
			env,
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup);
		this.yarnConfig = new YarnConfiguration();
		this.workerNodeMap = new ConcurrentHashMap<>();
		this.workerResources = new HashMap<>();
		this.containerResources = new HashMap<>();
		this.pendingWorkerNums = new HashMap<>();

		this.yarnMinAllocationMB = yarnConfig.getInt(
			YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
			YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
		this.yarnMinAllocationVcore = yarnConfig.getInt(
			YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
			YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);

		final int yarnHeartbeatIntervalMS = flinkConfig.getInteger(
				YarnConfigOptions.HEARTBEAT_DELAY_SECONDS) * 1000;

		final long yarnExpiryIntervalMS = yarnConfig.getLong(
				YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
				YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);

		if (yarnHeartbeatIntervalMS >= yarnExpiryIntervalMS) {
			log.warn("The heartbeat interval of the Flink Application master ({}) is greater " +
					"than YARN's expiry interval ({}). The application is likely to be killed by YARN.",
					yarnHeartbeatIntervalMS, yarnExpiryIntervalMS);
		}
		yarnHeartbeatIntervalMillis = yarnHeartbeatIntervalMS;
		containerRequestHeartbeatIntervalMillis = flinkConfig.getInteger(YarnConfigOptions.CONTAINER_REQUEST_HEARTBEAT_INTERVAL_MILLISECONDS);

		this.webInterfaceUrl = webInterfaceUrl;
	}

	protected AMRMClientAsync<AMRMClient.ContainerRequest> createAndStartResourceManagerClient(
			YarnConfiguration yarnConfiguration,
			int yarnHeartbeatIntervalMillis,
			@Nullable String webInterfaceUrl) throws Exception {
		AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(
			yarnHeartbeatIntervalMillis,
			this);

		resourceManagerClient.init(yarnConfiguration);
		resourceManagerClient.start();

		//TODO: change akka address to tcp host and port, the getAddress() interface should return a standard tcp address
		Tuple2<String, Integer> hostPort = parseHostPort(getAddress());

		final int restPort;

		if (webInterfaceUrl != null) {
			final int lastColon = webInterfaceUrl.lastIndexOf(':');

			if (lastColon == -1) {
				restPort = -1;
			} else {
				restPort = Integer.valueOf(webInterfaceUrl.substring(lastColon + 1));
			}
		} else {
			restPort = -1;
		}

		final RegisterApplicationMasterResponse registerApplicationMasterResponse =
			resourceManagerClient.registerApplicationMaster(hostPort.f0, restPort, webInterfaceUrl);
		getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		return resourceManagerClient;
	}

	private void getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final List<Container> containersFromPreviousAttempts =
			new RegisterApplicationMasterResponseReflector(log).getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		log.info("Recovered {} containers from previous attempts ({}).", containersFromPreviousAttempts.size(), containersFromPreviousAttempts);

		for (final Container container : containersFromPreviousAttempts) {
			workerNodeMap.put(new ResourceID(container.getId().toString()), new YarnWorkerNode(container));
		}
	}

	protected NMClientAsync createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
		// create the client to communicate with the node managers
		NMClientAsync nodeManagerClient = NMClientAsync.createNMClientAsync(this);
		nodeManagerClient.init(yarnConfiguration);
		nodeManagerClient.start();
		return nodeManagerClient;
	}

	@Override
	protected Configuration loadClientConfiguration() {
		return GlobalConfiguration.loadConfiguration(env.get(ApplicationConstants.Environment.PWD.key()));
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		try {
			resourceManagerClient = createAndStartResourceManagerClient(
				yarnConfig,
				yarnHeartbeatIntervalMillis,
				webInterfaceUrl);
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start resource manager client.", e);
		}

		nodeManagerClient = createAndStartNodeManagerClient(yarnConfig);
	}

	@Override
	public CompletableFuture<Void> onStop() {
		// shut down all components
		Throwable firstException = null;

		if (resourceManagerClient != null) {
			try {
				resourceManagerClient.stop();
			} catch (Throwable t) {
				firstException = t;
			}
		}

		if (nodeManagerClient != null) {
			try {
				nodeManagerClient.stop();
			} catch (Throwable t) {
				firstException = ExceptionUtils.firstOrSuppressed(t, firstException);
			}
		}

		return getStopTerminationFutureOrCompletedExceptionally(firstException);
	}

	@Override
	protected void internalDeregisterApplication(
			ApplicationStatus finalStatus,
			@Nullable String diagnostics) {

		// first, de-register from YARN
		FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
		log.info("Unregister application from the YARN Resource Manager with final status {}.", yarnStatus);

		final Optional<URL> historyServerURL = HistoryServerUtils.getHistoryServerURL(flinkConfig);

		final String appTrackingUrl = historyServerURL.map(URL::toString).orElse("");

		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, diagnostics, appTrackingUrl);
		} catch (Throwable t) {
			log.error("Could not unregister the application master.", t);
		}

		Utils.deleteApplicationFiles(env);
	}

	@Override
	public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
		requestYarnContainer(workerResourceSpec);
		return true;
	}

	@VisibleForTesting
	Resource getContainerResource(WorkerResourceSpec workerResourceSpec) {
		return containerResources.computeIfAbsent(workerResourceSpec, workerSpec -> {
			// TODO: need to unset process/flink memory size from configuration if dynamic worker resource is activated
			final TaskExecutorProcessSpec processSpec =
				TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerSpec);
			final Resource resource = getNormalizedResource(
				processSpec.getTotalProcessMemorySize().getMebiBytes(),
				processSpec.getCpuCores().getValue().intValue());
			workerResources.computeIfAbsent(resource, ignore -> new HashSet<>()).add(workerResourceSpec);
			return resource;
		});
	}

	private Resource getNormalizedResource(int memoryMB, int vcore) {
		return Resource.newInstance(
			normalize(memoryMB, yarnMinAllocationMB),
			normalize(vcore, yarnMinAllocationVcore));
	}

	/**
	 * Normalize to the minimum integer that is greater or equal to 'value' and is integer multiple of 'unitValue'.
	 */
	private int normalize(int value, int unitValue) {
		if (value < unitValue) {
			return unitValue;
		}

		if (value % unitValue == 0) {
			return value;
		}

		return (value / unitValue + 1) * unitValue;
	}

	@Override
	public boolean stopWorker(final YarnWorkerNode workerNode) {
		final Container container = workerNode.getContainer();
		log.info("Stopping container {}.", container.getId());
		nodeManagerClient.stopContainerAsync(container.getId(), container.getNodeId());
		resourceManagerClient.releaseAssignedContainer(container.getId());
		workerNodeMap.remove(workerNode.getResourceID());
		return true;
	}

	@Override
	protected YarnWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodeMap.get(resourceID);
	}

	// ------------------------------------------------------------------------
	//  AMRMClientAsync CallbackHandler methods
	// ------------------------------------------------------------------------

	@Override
	public float getProgress() {
		// Temporarily need not record the total size of asked and allocated containers
		return 1;
	}

	@Override
	public void onContainersCompleted(final List<ContainerStatus> statuses) {
		runAsync(() -> {
				log.debug("YARN ResourceManager reported the following containers completed: {}.", statuses);
				for (final ContainerStatus containerStatus : statuses) {

					final ResourceID resourceId = new ResourceID(containerStatus.getContainerId().toString());
					final YarnWorkerNode yarnWorkerNode = workerNodeMap.remove(resourceId);

					if (yarnWorkerNode != null) {
						// Container completed unexpectedly ~> start a new one
						requestYarnContainerIfRequired(yarnWorkerNode.getContainer().getResource());
					}
					// Eagerly close the connection with task manager.
					closeTaskManagerConnection(resourceId, new Exception(containerStatus.getDiagnostics()));
				}
			}
		);
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		runAsync(() -> {
			log.info("Received {} containers.", containers.size());

			groupContainerByResource(containers).forEach(this::onContainersOfResourceAllocated);

			// if we are waiting for no further containers, we can go to the
			// regular heartbeat interval
			if (pendingWorkerNums.values().stream().reduce(0, Integer::sum) <= 0) {
				resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
			}
		});
	}

	private Map<Resource, List<Container>> groupContainerByResource(List<Container> containers) {
		return containers.stream().collect(Collectors.groupingBy(Container::getResource));
	}

	private void onContainersOfResourceAllocated(Resource resource, List<Container> containers) {
		final List<WorkerResourceSpec> pendingWorkerResourceSpecs = workerResources.get(resource).stream()
			.flatMap(spec -> Collections.nCopies(pendingWorkerNums.getOrDefault(spec, 0), spec).stream())
			.collect(Collectors.toList());

		int numPending = pendingWorkerResourceSpecs.size();
		log.info("Received {} containers with resource {}, {} pending container requests.",
			containers.size(),
			resource,
			numPending);

		final Iterator<Container> containerIterator = containers.iterator();
		final Iterator<WorkerResourceSpec> workerResourceIterator = pendingWorkerResourceSpecs.iterator();
		final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator =
			getPendingRequests(resource, pendingWorkerResourceSpecs.size()).iterator();

		int numAccepted = 0;
		while (containerIterator.hasNext() && workerResourceIterator.hasNext()) {
			WorkerResourceSpec workerResourceSpec = workerResourceIterator.next();
			startTaskExecutorInContainer(containerIterator.next(), workerResourceSpec);
			removeContainerRequest(pendingRequestsIterator.next());
			pendingWorkerNums.compute(workerResourceSpec, (ignored, num) -> {
				Preconditions.checkNotNull(num);
				Preconditions.checkState(num > 0);
				return num - 1;
			});
			numAccepted++;
		}
		numPending -= numAccepted;

		int numExcess = 0;
		while (containerIterator.hasNext()) {
			returnExcessContainer(containerIterator.next());
			numExcess++;
		}

		log.info("Accepted {} requested containers, returned {} excess containers, {} pending container requests of resource {}.",
			numAccepted, numExcess, numPending, resource);
	}

	private void startTaskExecutorInContainer(Container container, WorkerResourceSpec workerResourceSpec) {
		final String containerIdStr = container.getId().toString();
		final ResourceID resourceId = new ResourceID(containerIdStr);

		workerNodeMap.put(resourceId, new YarnWorkerNode(container));

		try {
			// Context information used to start a TaskExecutor Java process
			ContainerLaunchContext taskExecutorLaunchContext = createTaskExecutorLaunchContext(
				containerIdStr,
				container.getNodeId().getHost(),
				TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec));

			nodeManagerClient.startContainerAsync(container, taskExecutorLaunchContext);
		} catch (Throwable t) {
			releaseFailedContainerAndRequestNewContainerIfRequired(container.getId(), t);
		}
	}

	private void releaseFailedContainerAndRequestNewContainerIfRequired(ContainerId containerId, Throwable throwable) {
		validateRunsInMainThread();

		log.error("Could not start TaskManager in container {}.", containerId, throwable);

		final ResourceID resourceId = new ResourceID(containerId.toString());
		// release the failed container
		YarnWorkerNode yarnWorkerNode = workerNodeMap.remove(resourceId);
		resourceManagerClient.releaseAssignedContainer(containerId);
		// and ask for a new one
		requestYarnContainerIfRequired(yarnWorkerNode.getContainer().getResource());
	}

	private void returnExcessContainer(Container excessContainer) {
		log.info("Returning excess container {}.", excessContainer.getId());
		resourceManagerClient.releaseAssignedContainer(excessContainer.getId());
	}

	private void removeContainerRequest(AMRMClient.ContainerRequest pendingContainerRequest) {
		log.info("Removing container request {}.", pendingContainerRequest);
		resourceManagerClient.removeContainerRequest(pendingContainerRequest);
	}

	private Collection<AMRMClient.ContainerRequest> getPendingRequests(Resource resource, int expectedNum) {
		final List<? extends Collection<AMRMClient.ContainerRequest>> matchingRequests = resourceManagerClient.getMatchingRequests(
			RM_REQUEST_PRIORITY,
			ResourceRequest.ANY,
			resource);

		final Collection<AMRMClient.ContainerRequest> matchingContainerRequests;

		if (matchingRequests.isEmpty()) {
			matchingContainerRequests = Collections.emptyList();
		} else {
			final Collection<AMRMClient.ContainerRequest> collection = matchingRequests.get(0);
			matchingContainerRequests = new ArrayList<>(collection);
		}

		Preconditions.checkState(
			matchingContainerRequests.size() == expectedNum,
			"The RMClient's and YarnResourceManagers internal state about the number of pending container requests for resource %s has diverged. " +
				"Number client's pending container requests %s != Number RM's pending container requests %s.",
			resource, matchingContainerRequests.size(), expectedNum);

		return matchingContainerRequests;
	}

	@Override
	public void onShutdownRequest() {
		onFatalError(new ResourceManagerException(ERROR_MASSAGE_ON_SHUTDOWN_REQUEST));
	}

	@Override
	public void onNodesUpdated(List<NodeReport> list) {
		// We are not interested in node updates
	}

	@Override
	public void onError(Throwable error) {
		onFatalError(error);
	}

	// ------------------------------------------------------------------------
	//  NMClientAsync CallbackHandler methods
	// ------------------------------------------------------------------------
	@Override
	public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
		log.debug("Succeeded to call YARN Node Manager to start container {}.", containerId);
	}

	@Override
	public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
		// We are not interested in getting container status
	}

	@Override
	public void onContainerStopped(ContainerId containerId) {
		log.debug("Succeeded to call YARN Node Manager to stop container {}.", containerId);
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		runAsync(() -> releaseFailedContainerAndRequestNewContainerIfRequired(containerId, t));
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
		// We are not interested in getting container status
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable throwable) {
		log.warn("Error while calling YARN Node Manager to stop container {}.", containerId, throwable);
	}

	// ------------------------------------------------------------------------
	//  Utility methods
	// ------------------------------------------------------------------------

	/**
	 * Converts a Flink application status enum to a YARN application status enum.
	 * @param status The Flink application status.
	 * @return The corresponding YARN application status.
	 */
	private FinalApplicationStatus getYarnStatus(ApplicationStatus status) {
		if (status == null) {
			return FinalApplicationStatus.UNDEFINED;
		}
		else {
			switch (status) {
				case SUCCEEDED:
					return FinalApplicationStatus.SUCCEEDED;
				case FAILED:
					return FinalApplicationStatus.FAILED;
				case CANCELED:
					return FinalApplicationStatus.KILLED;
				default:
					return FinalApplicationStatus.UNDEFINED;
			}
		}
	}

	// parse the host and port from akka address,
	// the akka address is like akka.tcp://flink@100.81.153.180:49712/user/$a
	private static Tuple2<String, Integer> parseHostPort(String address) {
		String[] hostPort = address.split("@")[1].split(":");
		String host = hostPort[0];
		String port = hostPort[1].split("/")[0];
		return new Tuple2<>(host, Integer.valueOf(port));
	}

	/**
	 * Request new container if pending containers cannot satisfy pending slot requests.
	 */
	private void requestYarnContainerIfRequired(Resource containerResource) {
		getPendingWorkerNums().entrySet().stream()
			.filter(entry ->
				getContainerResource(entry.getKey()).equals(containerResource) &&
				entry.getValue() > pendingWorkerNums.getOrDefault(entry.getKey(), 0))
			.findAny()
			.ifPresent(entry -> requestYarnContainer(entry.getKey()));
	}

	private void requestYarnContainer(WorkerResourceSpec workerResourceSpec) {
		resourceManagerClient.addContainerRequest(getContainerRequest(workerResourceSpec));

		// make sure we transmit the request fast and receive fast news of granted allocations
		resourceManagerClient.setHeartbeatInterval(containerRequestHeartbeatIntervalMillis);
		int numPendingWorkers = pendingWorkerNums.compute(workerResourceSpec,
			(ignore, num) -> num != null ? num + 1 : 1);

		log.info("Requesting new TaskExecutor container with resource {}. Number pending workers of this resource is {}.",
			workerResourceSpec,
			numPendingWorkers);
	}

	@Nonnull
	@VisibleForTesting
	AMRMClient.ContainerRequest getContainerRequest(WorkerResourceSpec workerResourceSpec) {
		return new AMRMClient.ContainerRequest(
			getContainerResource(workerResourceSpec),
			null,
			null,
			RM_REQUEST_PRIORITY);
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(
		String containerId,
		String host,
		TaskExecutorProcessSpec taskExecutorProcessSpec) throws Exception {

		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

		final ContaineredTaskManagerParameters taskManagerParameters =
				ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

		log.info("TaskExecutor {} will be started on {} with {}.",
			containerId,
			host,
			taskExecutorProcessSpec);

		final Configuration taskManagerConfig = BootstrapTools.cloneConfiguration(flinkConfig);

		final String taskManagerDynamicProperties =
			BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, taskManagerConfig);

		log.debug("TaskManager configuration: {}", taskManagerConfig);

		ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
			flinkConfig,
			yarnConfig,
			env,
			taskManagerParameters,
			taskManagerDynamicProperties,
			currDir,
			YarnTaskExecutorRunner.class,
			log);

		// set a special environment variable to uniquely identify this container
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_CONTAINER_ID, containerId);
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_NODE_ID, host);
		return taskExecutorLaunchContext;
	}

	@Override
	protected double getCpuCores(final Configuration configuration) {
		int fallback = configuration.getInteger(YarnConfigOptions.VCORES);
		double cpuCoresDouble = TaskExecutorProcessUtils.getCpuCoresWithFallback(configuration, fallback).getValue().doubleValue();
		@SuppressWarnings("NumericCastThatLosesPrecision")
		long cpuCoresLong = Math.max((long) Math.ceil(cpuCoresDouble), 1L);
		//noinspection FloatingPointEquality
		if (cpuCoresLong != cpuCoresDouble) {
			log.info(
				"The amount of cpu cores must be a positive integer on Yarn. Rounding {} up to the closest positive integer {}.",
				cpuCoresDouble,
				cpuCoresLong);
		}
		if (cpuCoresLong > Integer.MAX_VALUE) {
			throw new IllegalConfigurationException(String.format(
				"The amount of cpu cores %d cannot exceed Integer.MAX_VALUE: %d",
				cpuCoresLong,
				Integer.MAX_VALUE));
		}
		//noinspection NumericCastThatLosesPrecision
		return cpuCoresLong;
	}
}
