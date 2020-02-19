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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.runtime.resourcemanager.slotmanager.WorkerRequest;

import java.util.List;
import java.util.Map;

/**
 * Parameters to create a taskmanager pod.
 */
public class TaskManagerPodParameter {

	private final String podName;

	private final List<String> args;

	private final int taskManagerMemoryInMB;

	private final double taskManagerCpus;

	private final Map<String, String> environmentVariables;

	private final WorkerRequest.WorkerTypeID workerTypeId;

	public TaskManagerPodParameter(
			String podName,
			List<String> args,
			int taskManagerMemoryInMB,
			double taskManagerCpus,
			Map<String, String> environmentVariables,
			WorkerRequest.WorkerTypeID workerTypeId) {
		this.podName = podName;
		this.args = args;
		this.taskManagerMemoryInMB = taskManagerMemoryInMB;
		this.taskManagerCpus = taskManagerCpus;
		this.environmentVariables = environmentVariables;
		this.workerTypeId = workerTypeId;
	}

	public String getPodName() {
		return podName;
	}

	public List<String> getArgs() {
		return args;
	}

	public Map<String, String> getEnvironmentVariables() {
		return environmentVariables;
	}

	public int getTaskManagerMemoryInMB() {
		return taskManagerMemoryInMB;
	}

	public double getTaskManagerCpus() {
		return taskManagerCpus;
	}

	public WorkerRequest.WorkerTypeID getWorkerTypeId() {
		return workerTypeId;
	}
}
