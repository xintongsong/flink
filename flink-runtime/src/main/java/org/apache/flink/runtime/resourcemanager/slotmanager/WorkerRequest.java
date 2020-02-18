/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

/**
 * This describes the requirement of the worker that SlotManager requests from ResourceManager.
 */
public class WorkerRequest {

	private final WorkerTypeID workerTypeId;

	private final TaskExecutorProcessSpec taskExecutorProcessSpec;

	public WorkerRequest(final WorkerTypeID workerTypeID, final TaskExecutorProcessSpec taskExecutorProcessSpec) {
		this.workerTypeId = Preconditions.checkNotNull(workerTypeID);
		this.taskExecutorProcessSpec = Preconditions.checkNotNull(taskExecutorProcessSpec);
	}

	public WorkerTypeID getWorkerTypeId() {
		return workerTypeId;
	}

	public TaskExecutorProcessSpec getTaskExecutorProcessSpec() {
		return taskExecutorProcessSpec;
	}

	@Override
	public String toString() {
		return "WorkerRequest{" +
			"workerTypeId=" + workerTypeId +
			",taskExecutorProcessSpec=" + taskExecutorProcessSpec +
			"}";
	}

	/**
	 * This uniquely identify a worker type, i.e. a group of identical workers.
	 */
	public static class WorkerTypeID extends AbstractID {
		private static final long serialVersionUID = 1L;

		public WorkerTypeID() {
			super();
		}
	}
}
