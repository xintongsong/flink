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
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
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
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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

	/** Map from pod name to worker resource. */
	private final Map<String, WorkerResourceSpec> podWorkerResources;

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

		this.podWorkerResources = new HashMap<>();
	}

	@Override
	protected Configuration loadClientConfiguration() {
		return GlobalConfiguration.loadConfiguration();
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		recoverWorkerNodesFromPreviousAttempts();

		kubeClient.watchPodsAndDoCallback(KubernetesUtils.getTaskManagerLabels(clusterId), this);
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
	public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
		LOG.info("Starting new worker with worker resource spec, {}", workerResourceSpec);
		requestKubernetesPod(workerResourceSpec);
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
		runAsync(() -> {
			pods.forEach(pod -> {
				WorkerResourceSpec workerResourceSpec = podWorkerResources.get(pod.getName());
				final int pendingNum = pendingWorkerCounter.getNum(workerResourceSpec);
				if (pendingNum > 0) {
					pendingWorkerCounter.decreaseAndGet(workerResourceSpec);
					final KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(pod.getName()));
					workerNodes.putIfAbsent(worker.getResourceID(), worker);
				}
				log.info("Received new TaskManager pod: {}", pod.getName());
			});
			log.info("Received {} new TaskManager pods. Remaining pending pod requests: {}",
				pods.size(), pendingWorkerCounter.getTotalNum());
		});
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
		final List<KubernetesPod> podList = kubeClient.getPodsWithLabels(KubernetesUtils.getTaskManagerLabels(clusterId));
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

	private void requestKubernetesPod(WorkerResourceSpec workerResourceSpec) {
		final KubernetesTaskManagerParameters parameters =
			createKubernetesTaskManagerParameters(workerResourceSpec);

		podWorkerResources.put(parameters.getPodName(), workerResourceSpec);
		final int pendingWorkerNum = pendingWorkerCounter.increaseAndGet(workerResourceSpec);

		log.info("Requesting new TaskManager pod with <{},{}>. Number pending requests {}.",
			parameters.getTaskManagerMemoryMB(),
			parameters.getTaskManagerCPU(),
			pendingWorkerNum);
		log.info("TaskManager {} will be started with {}.", parameters.getPodName(), workerResourceSpec);

		final KubernetesPod taskManagerPod =
			KubernetesTaskManagerFactory.createTaskManagerComponent(parameters);
		kubeClient.createTaskManagerPod(taskManagerPod);
	}

	private KubernetesTaskManagerParameters createKubernetesTaskManagerParameters(WorkerResourceSpec workerResourceSpec) {
		// TODO: need to unset process/flink memory size from configuration if dynamic worker resource is activated
		final TaskExecutorProcessSpec taskExecutorProcessSpec =
			TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec);

		final String podName = String.format(
			TASK_MANAGER_POD_FORMAT,
			clusterId,
			currentMaxAttemptId,
			++currentMaxPodId);

		final ContaineredTaskManagerParameters taskManagerParameters =
			ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

		final String dynamicProperties =
			BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, flinkConfig);

		return new KubernetesTaskManagerParameters(
			flinkConfig,
			podName,
			dynamicProperties,
			taskManagerParameters);
	}

	/**
	 * Request new pod if pending pods cannot satisfy pending slot requests.
	 */
	private void requestKubernetesPodIfRequired(WorkerResourceSpec workerResourceSpec) {
		final int requiredTaskManagers = getPendingWorkerNums().get(workerResourceSpec);
		final int pendingWorkerNum = pendingWorkerCounter.getNum(workerResourceSpec);

		if (requiredTaskManagers > pendingWorkerNum) {
			requestKubernetesPod(workerResourceSpec);
		}
	}

	private void removePodIfTerminated(KubernetesPod pod) {
		if (pod.isTerminated()) {
			kubeClient.stopPod(pod.getName());
			final KubernetesWorkerNode kubernetesWorkerNode = workerNodes.remove(new ResourceID(pod.getName()));
			if (kubernetesWorkerNode != null) {
				requestKubernetesPodIfRequired(podWorkerResources.get(pod.getName()));
			}
		}
	}

	protected FlinkKubeClient createFlinkKubeClient() {
		return KubeClientFactory.fromConfiguration(flinkConfig);
	}
}
