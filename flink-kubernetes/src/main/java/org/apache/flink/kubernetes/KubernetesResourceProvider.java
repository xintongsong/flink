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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.resourcemanager.active.AbstractResourceProvider;
import org.apache.flink.runtime.resourcemanager.active.ResourceEventListener;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * KubernetesResourceProvider.
 */
public class KubernetesResourceProvider extends AbstractResourceProvider<KubernetesWorkerNode>
	implements FlinkKubeClient.PodCallbackHandler {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesResourceProvider.class);

	/** The taskmanager pod name pattern is {clusterId}-{taskmanager}-{attemptId}-{podIndex}. */
	private static final String TASK_MANAGER_POD_FORMAT = "%s-taskmanager-%d-%d";

	private final String clusterId;

	private final FlinkKubeClient kubeClient;

	/**
	 * Request resource futures, keyed by pod names.
	 */
	private final Map<String, CompletableFuture<KubernetesWorkerNode>> requestResourceFutures;

	/** When ResourceManager failover, the max attempt should recover. */
	private long currentMaxAttemptId = 0;

	/** Current max pod index. When creating a new pod, it should increase one. */
	private long currentMaxPodId = 0;

	private KubernetesWatch podsWatch;

	public KubernetesResourceProvider(
			Configuration flinkConfig,
			FlinkKubeClient kubeClient,
			KubernetesResourceManagerConfiguration configuration) {
		super(
			flinkConfig,
			GlobalConfiguration.loadConfiguration());

		this.clusterId = configuration.getClusterId();
		this.kubeClient = kubeClient;
		requestResourceFutures = new HashMap<>();
	}

	// ------------------------------------------------------------------------
	//  ResourceProvider
	// ------------------------------------------------------------------------

	@Override
	public void initialize(
			ResourceEventListener<KubernetesWorkerNode> resourceEventListener,
			ComponentMainThreadExecutor mainThreadExecutor) throws Throwable{
		setResourceEventListener(resourceEventListener);
		setMainThreadExecutor(mainThreadExecutor);

		recoverWorkerNodesFromPreviousAttempts();

		podsWatch = kubeClient.watchPodsAndDoCallback(
			KubernetesUtils.getTaskManagerLabels(clusterId),
			this);
	}

	@Override
	public void terminate() throws Throwable {
		// shut down all components
		Throwable throwable = null;

		try {
			podsWatch.close();
		} catch (Throwable t) {
			throwable = t;
		}

		try {
			kubeClient.close();
		} catch (Throwable t) {
			throwable = ExceptionUtils.firstOrSuppressed(t, throwable);
		}

		ExceptionUtils.tryRethrowThrowable(throwable);
	}

	@Override
	public void deregisterApplication(ApplicationStatus finalStatus, @Nullable String diagnostics) {
		LOG.info(
			"Stopping kubernetes cluster, clusterId: {}, diagnostics: {}",
			clusterId,
			diagnostics == null ? "" : diagnostics);
		kubeClient.stopAndCleanupCluster(clusterId);
	}

	@Override
	public CompletableFuture<KubernetesWorkerNode> requestResource(TaskExecutorProcessSpec taskExecutorProcessSpec) {
		final KubernetesTaskManagerParameters parameters =
			createKubernetesTaskManagerParameters(taskExecutorProcessSpec);
		final KubernetesPod taskManagerPod =
			KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(parameters);
		final String podName = taskManagerPod.getName();
		final CompletableFuture<KubernetesWorkerNode> requestResourceFuture = new CompletableFuture<>();

		requestResourceFutures.put(podName, requestResourceFuture);

		log.info("Creating new TaskManager pod with name {} and resource <{},{}>.",
			podName,
			parameters.getTaskManagerMemoryMB(),
			parameters.getTaskManagerCPU());

		// TODO: add retry interval

		kubeClient.createTaskManagerPod(taskManagerPod)
			.whenCompleteAsync(
				(ignore, throwable) -> {
					if (throwable != null) {
						log.warn("Could not create pod {}, exception: {}", podName, throwable);
						CompletableFuture<KubernetesWorkerNode> future =
							requestResourceFutures.remove(taskManagerPod.getName());
						if (future != null) {
							future.completeExceptionally(throwable);
						}
					} else {
						log.info("Pod {} is created.", podName);
					}
				},
				getMainThreadExecutor());
		return requestResourceFuture;
	}

	@Override
	public void releaseResource(KubernetesWorkerNode worker) {
		final String podName = worker.getResourceID().toString();

		log.info("Stopping TaskManager pod {}.", podName);

		removePod(podName);
	}

	// ------------------------------------------------------------------------
	//  FlinkKubeClient.PodCallbackHandler
	// ------------------------------------------------------------------------

	@Override
	public void onAdded(List<KubernetesPod> pods) {
		getMainThreadExecutor().execute(() -> {
			for (KubernetesPod pod : pods) {
				final String podName = pod.getName();
				final CompletableFuture<KubernetesWorkerNode> requestResourceFuture = requestResourceFutures.remove(podName);

				if (requestResourceFuture == null) {
					log.debug("Ignore TaskManager pod that is already added: {}", podName);
					continue;
				}

				log.info("Received new TaskManager pod: {}", podName);
				requestResourceFuture.complete(new KubernetesWorkerNode(new ResourceID(podName)));
			}
		});
	}

	@Override
	public void onModified(List<KubernetesPod> pods) {
		terminatedPodsInMainThread(pods);
	}

	@Override
	public void onDeleted(List<KubernetesPod> pods) {
		terminatedPodsInMainThread(pods);
	}

	@Override
	public void onError(List<KubernetesPod> pods) {
		terminatedPodsInMainThread(pods);
	}

	@Override
	public void handleFatalError(Throwable throwable) {
		getMainThreadExecutor().execute(() -> getResourceEventListener().onError(throwable));
	}

	// ------------------------------------------------------------------------
	//  Internal
	// ------------------------------------------------------------------------

	private void recoverWorkerNodesFromPreviousAttempts() throws ResourceManagerException {
		final List<KubernetesPod> podList = kubeClient.getPodsWithLabels(KubernetesUtils.getTaskManagerLabels(clusterId));
		final List<KubernetesWorkerNode> recoveredWorkers = new ArrayList<>();

		for (KubernetesPod pod : podList) {
				final KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(pod.getName()));
			recoveredWorkers.add(worker);
			final long attempt = worker.getAttempt();
			if (attempt > currentMaxAttemptId) {
				currentMaxAttemptId = attempt;
			}
		}

		LOG.info("Recovered {} pods from previous attempts, current attempt id is {}.",
			recoveredWorkers.size(),
			++currentMaxAttemptId);

		getResourceEventListener().onPreviousAttemptWorkersRecovered(recoveredWorkers);
	}

	private KubernetesTaskManagerParameters createKubernetesTaskManagerParameters(TaskExecutorProcessSpec taskExecutorProcessSpec) {
		final String podName = String.format(
			TASK_MANAGER_POD_FORMAT,
			clusterId,
			currentMaxAttemptId,
			++currentMaxPodId);

		final ContaineredTaskManagerParameters taskManagerParameters =
			ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

		final Configuration taskManagerConfig = new Configuration(flinkConfig);
		taskManagerConfig.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, podName);

		final String dynamicProperties =
			BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, taskManagerConfig);

		return new KubernetesTaskManagerParameters(
			flinkConfig,
			podName,
			dynamicProperties,
			taskManagerParameters,
			ExternalResourceUtils.getExternalResources(flinkConfig, KubernetesConfigOptions.EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX));
	}

	private void terminatedPodsInMainThread(List<KubernetesPod> pods) {
		getMainThreadExecutor().execute(() -> {
			for (KubernetesPod pod : pods) {
				if (pod.isTerminated()) {
					final String podName = pod.getName();
					log.info("TaskManager pod {} is terminated.", podName);
					getResourceEventListener().onWorkerTerminated(new ResourceID(podName));
					removePod(podName);
				}
			}
		});
	}

	private void removePod(String podName) {
		kubeClient.stopPod(podName)
			.whenComplete(
				(ignore, throwable) -> {
					if (throwable != null) {
						log.warn("Could not remove TaskManager pod {}, exception: {}", podName, throwable);
					}
				});
	}
}
