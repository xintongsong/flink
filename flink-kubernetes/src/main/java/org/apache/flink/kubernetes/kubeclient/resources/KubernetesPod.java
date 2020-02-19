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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.resourcemanager.slotmanager.WorkerRequest;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.Pod;

/**
 * Represent KubernetesPod resource in kubernetes.
 */
public class KubernetesPod extends KubernetesResource<Pod> {

	public KubernetesPod(Configuration flinkConfig) {
		this(flinkConfig, new Pod());
	}

	public KubernetesPod(Configuration flinkConfig, Pod pod) {
		super(flinkConfig, pod);
	}

	public String getName() {
		return this.getInternalResource().getMetadata().getName();
	}

	public boolean isTerminated() {
		if (getInternalResource().getStatus() != null) {
			return getInternalResource()
				.getStatus()
				.getContainerStatuses()
				.stream()
				.anyMatch(e -> e.getState() != null && e.getState().getTerminated() != null);
		}
		return false;
	}

	public WorkerRequest.WorkerTypeID getWorkerTypeId() {
		String workerTypeIdLabel = Preconditions.checkNotNull(
			this.getInternalResource().getMetadata().getLabels().get(Constants.LABEL_WORKER_TYPE_ID_KEY),
			"Worker pod should always be associated with a '%s' label.", Constants.LABEL_WORKER_TYPE_ID_KEY);
		return WorkerRequest.WorkerTypeID.fromHexString(workerTypeIdLabel);
	}
}
