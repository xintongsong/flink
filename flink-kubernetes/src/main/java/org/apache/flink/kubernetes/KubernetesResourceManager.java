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

package org.apache.flink.kubernetes;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.TaskManagerPodParameter;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.taskmanager.KubernetesTaskExecutorRunner;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.entrypoint.parser.CommandLineOptions;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.WorkerRequest;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Kubernetes specific implementation of the {@link ResourceManager}.
 */
public class KubernetesResourceManager extends ActiveResourceManager<KubernetesWorkerNode>
	implements FlinkKubeClient.PodCallbackHandler {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesResourceManager.class);

	/** The taskmanager pod name pattern is {clusterId}-{taskmanager}-{attemptId}-{podIndex}. */
	private static final String TASK_MANAGER_POD_FORMAT = "%s-taskmanager-%d-%d";

	private final Map<ResourceID, KubernetesWorkerNode> workerNodes = new HashMap<>();

	/** When ResourceManager failover, the max attempt should recover. */
	private long currentMaxAttemptId = 0;

	/** Current max pod index. When creating a new pod, it should increase one. */
	private long currentMaxPodId = 0;

	private final String clusterId;

	private final FlinkKubeClient kubeClient;

	/** Pending worker requests of each type. */
	private final Map<WorkerRequest.WorkerTypeID, WorkerRequest> pendingWorkerRequests;

	/** Number of pending workers of each type. */
	private final Map<WorkerRequest.WorkerTypeID, Integer> pendingWorkerNums;

	public KubernetesResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			Configuration flinkConfig,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup) {
		super(
			flinkConfig,
			System.getenv(),
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
		this.clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);

		this.kubeClient = createFlinkKubeClient();

		this.pendingWorkerRequests = new HashMap<>();
		this.pendingWorkerNums = new HashMap<>();
	}

	@Override
	protected Configuration loadClientConfiguration() {
		return GlobalConfiguration.loadConfiguration();
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		recoverWorkerNodesFromPreviousAttempts();

		kubeClient.watchPodsAndDoCallback(getTaskManagerLabels(), this);
	}

	@Override
	public CompletableFuture<Void> onStop() {
		// shut down all components
		Throwable exception = null;

		try {
			kubeClient.close();
		} catch (Throwable t) {
			exception = t;
		}

		return getStopTerminationFutureOrCompletedExceptionally(exception);
	}

	@Override
	protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String diagnostics) {
		LOG.info(
			"Stopping kubernetes cluster, clusterId: {}, diagnostics: {}",
			clusterId,
			diagnostics == null ? "" : diagnostics);
		kubeClient.stopAndCleanupCluster(clusterId);
	}

	@Override
	public boolean startNewWorker(WorkerRequest workerRequest) {
		LOG.info("Starting new worker with process spec, {}", workerRequest.getTaskExecutorProcessSpec());
		requestKubernetesPod(workerRequest);
		return true;
	}

	@Override
	protected KubernetesWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodes.get(resourceID);
	}

	@Override
	public boolean stopWorker(final KubernetesWorkerNode worker) {
		LOG.info("Stopping Worker {}.", worker.getResourceID());
		workerNodes.remove(worker.getResourceID());
		try {
			kubeClient.stopPod(worker.getResourceID().toString());
		} catch (Exception e) {
			kubeClient.handleException(e);
			return false;
		}
		return true;
	}

	@Override
	public void onAdded(List<KubernetesPod> pods) {
		runAsync(() -> groupPodsByWorkerTyps(pods).forEach((workerTypeId, podsOfType) -> {
			if (!pendingWorkerRequests.containsKey(workerTypeId)) {
				log.info("Ignore {} pods of unrecognized worker type id {}. "
						+ "This usually indicates receiving of legacy pod added event from previous attempt.",
					podsOfType.size(),
					workerTypeId);
				return;
			}

			int pendingWorkerNum = pendingWorkerNums.getOrDefault(workerTypeId, 0);
			Preconditions.checkState(pendingWorkerNum >= podsOfType.size(),
				"Should never receive more pods then pending requests.");

			for (KubernetesPod pod : podsOfType) {
				pendingWorkerNum--;
				final KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(pod.getName()));
				workerNodes.putIfAbsent(worker.getResourceID(), worker);

				log.info("Received new TaskManager pod: {} - Remaining pending pod requests: {}",
					pod.getName(), pendingWorkerNum);
			}

			pendingWorkerNums.put(workerTypeId, pendingWorkerNum);
		}));
	}

	private Map<WorkerRequest.WorkerTypeID, List<KubernetesPod>> groupPodsByWorkerTyps(final List<KubernetesPod> pods) {
		Map<WorkerRequest.WorkerTypeID, List<KubernetesPod>> podsByWorkerType = new HashMap<>();
		pods.forEach(pod -> {
			List<KubernetesPod> list = podsByWorkerType.computeIfAbsent(
				pod.getWorkerTypeId(), ignored -> new ArrayList<>());
			list.add(pod);
		});
		return podsByWorkerType;
	}

	@Override
	public void onModified(List<KubernetesPod> pods) {
		runAsync(() -> pods.forEach(this::removePodIfTerminated));
	}

	@Override
	public void onDeleted(List<KubernetesPod> pods) {
		runAsync(() -> pods.forEach(this::removePodIfTerminated));
	}

	@Override
	public void onError(List<KubernetesPod> pods) {
		runAsync(() -> pods.forEach(this::removePodIfTerminated));
	}

	@VisibleForTesting
	Map<ResourceID, KubernetesWorkerNode> getWorkerNodes() {
		return workerNodes;
	}

	private void recoverWorkerNodesFromPreviousAttempts() throws ResourceManagerException {
		final List<KubernetesPod> podList = kubeClient.getPodsWithLabels(getTaskManagerLabels());
		for (KubernetesPod pod : podList) {
			final KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(pod.getName()));
			workerNodes.put(worker.getResourceID(), worker);
			final long attempt = worker.getAttempt();
			if (attempt > currentMaxAttemptId) {
				currentMaxAttemptId = attempt;
			}
		}

		log.info("Recovered {} pods from previous attempts, current attempt id is {}.",
			workerNodes.size(),
			++currentMaxAttemptId);
	}

	private void requestKubernetesPod(WorkerRequest workerRequest) {
		pendingWorkerRequests.put(workerRequest.getWorkerTypeId(), workerRequest);
		int pendingWorkerNum = pendingWorkerNums.compute(workerRequest.getWorkerTypeId(),
			(workerTypeId, num) -> num == null ? 1 : (num + 1));

		final TaskManagerPodParameter parameter = createTaskManagerPodParameters(workerRequest);

		log.info("Requesting new TaskManager pod with <{},{}>. Number pending requests {}.",
			parameter.getTaskManagerMemoryInMB(),
			parameter.getTaskManagerCpus(),
			pendingWorkerNum);
		log.info("TaskManager {} will be started with {}.", parameter.getPodName(), workerRequest.getTaskExecutorProcessSpec());

		kubeClient.createTaskManagerPod(parameter);
	}

	private TaskManagerPodParameter createTaskManagerPodParameters(final WorkerRequest workerRequest) {
		final TaskExecutorProcessSpec taskExecutorProcessSpec = workerRequest.getTaskExecutorProcessSpec();

		final String podName = String.format(
			TASK_MANAGER_POD_FORMAT,
			clusterId,
			currentMaxAttemptId,
			++currentMaxPodId);

		final ContaineredTaskManagerParameters taskManagerParameters =
			ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);
		final HashMap<String, String> env = new HashMap<>();
		env.put(Constants.ENV_FLINK_POD_NAME, podName);
		env.putAll(taskManagerParameters.taskManagerEnv());

		final List<String> taskManagerStartCommand = getTaskManagerStartCommand(taskExecutorProcessSpec);

		return new TaskManagerPodParameter(
			podName,
			taskManagerStartCommand,
			taskExecutorProcessSpec.getTotalProcessMemorySize().getMebiBytes(),
			taskExecutorProcessSpec.getCpuCores().getValue().intValue(),
			env,
			workerRequest.getWorkerTypeId());
	}

	/**
	 * Request new pod if pending pods cannot satisfy pending slot requests.
	 */
	private void requestKubernetesPodIfRequired(WorkerRequest.WorkerTypeID workerTypeId) {
		final int requiredTaskManagers = getNumberRequiredTaskManagers(workerTypeId);
		final int pendingWorkerNum = pendingWorkerNums.getOrDefault(workerTypeId, 0);

		if (requiredTaskManagers > pendingWorkerNum) {
			requestKubernetesPod(pendingWorkerRequests.get(workerTypeId));
		}
	}

	private void removePodIfTerminated(KubernetesPod pod) {
		if (pod.isTerminated()) {
			kubeClient.stopPod(pod.getName());
			final KubernetesWorkerNode kubernetesWorkerNode = workerNodes.remove(new ResourceID(pod.getName()));
			if (kubernetesWorkerNode != null) {
				requestKubernetesPodIfRequired(pod.getWorkerTypeId());
			}
		}
	}

	private List<String> getTaskManagerStartCommand(TaskExecutorProcessSpec taskExecutorProcessSpec) {
		final String confDir = flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
		final boolean hasLogback = new File(confDir, Constants.CONFIG_FILE_LOGBACK_NAME).exists();
		final boolean hasLog4j = new File(confDir, Constants.CONFIG_FILE_LOG4J_NAME).exists();

		final String logDir = flinkConfig.getString(KubernetesConfigOptions.FLINK_LOG_DIR);

		final String mainClassArgs = "--" + CommandLineOptions.CONFIG_DIR_OPTION.getLongOpt() + " " +
			flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR) + " " +
			BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, flinkConfig);

		final String command = KubernetesUtils.getTaskManagerStartCommand(
			flinkConfig,
			taskExecutorProcessSpec,
			confDir,
			logDir,
			hasLogback,
			hasLog4j,
			KubernetesTaskExecutorRunner.class.getCanonicalName(),
			mainClassArgs);

		return Arrays.asList("/bin/bash", "-c", command);
	}

	/**
	 * Get task manager labels for the current Flink cluster. They could be used to watch the pods status.
	 *
	 * @return Task manager labels.
	 */
	private Map<String, String> getTaskManagerLabels() {
		final Map<String, String> labels = new HashMap<>();
		labels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		labels.put(Constants.LABEL_APP_KEY, clusterId);
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		return labels;
	}

	protected FlinkKubeClient createFlinkKubeClient() {
		return KubeClientFactory.fromConfiguration(flinkConfig);
	}

	@Override
	protected double getCpuCores(Configuration configuration) {
		double fallback = configuration.getDouble(KubernetesConfigOptions.TASK_MANAGER_CPU);
		return TaskExecutorProcessUtils.getCpuCoresWithFallback(configuration, fallback).getValue().doubleValue();
	}
}
