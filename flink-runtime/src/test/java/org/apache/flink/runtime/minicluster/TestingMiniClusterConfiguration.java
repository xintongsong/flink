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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.TaskManagerResource;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import static org.apache.flink.runtime.minicluster.RpcServiceSharing.SHARED;

/**
 * Configuration for the {@link TestingMiniCluster}.
 */
public class TestingMiniClusterConfiguration extends MiniClusterConfiguration {

	private final int numberDispatcherResourceManagerComponents;

	private final boolean localCommunication;

	public TestingMiniClusterConfiguration(
			Configuration configuration,
			int numTaskManagers,
			RpcServiceSharing rpcServiceSharing,
			@Nullable String commonBindAddress,
			int numberDispatcherResourceManagerComponents,
			boolean localCommunication) {
		super(configuration, numTaskManagers, rpcServiceSharing, commonBindAddress);
		this.numberDispatcherResourceManagerComponents = numberDispatcherResourceManagerComponents;
		this.localCommunication = localCommunication;
	}

	public int getNumberDispatcherResourceManagerComponents() {
		return numberDispatcherResourceManagerComponents;
	}

	public boolean isLocalCommunication() {
		return localCommunication;
	}

	/**
	 * Builder for the {@link TestingMiniClusterConfiguration}.
	 */
	public static class Builder {
		private Configuration configuration = new Configuration();
		private int numTaskManagers = 1;
		private int numSlotsPerTaskManager = 1;
		private RpcServiceSharing rpcServiceSharing = SHARED;
		private int numberDispatcherResourceManagerComponents = 1;
		private boolean localCommunication = false;

		@Nullable
		private String commonBindAddress = null;

		public Builder setConfiguration(Configuration configuration1) {
			this.configuration = Preconditions.checkNotNull(configuration1);
			return this;
		}

		public Builder setNumTaskManagers(int numTaskManagers) {
			this.numTaskManagers = numTaskManagers;
			return this;
		}

		public Builder setNumSlotsPerTaskManager(int numSlotsPerTaskManager) {
			this.numSlotsPerTaskManager = numSlotsPerTaskManager;
			return this;
		}

		public Builder setRpcServiceSharing(RpcServiceSharing rpcServiceSharing) {
			this.rpcServiceSharing = Preconditions.checkNotNull(rpcServiceSharing);
			return this;
		}

		public Builder setCommonBindAddress(String commonBindAddress) {
			this.commonBindAddress = commonBindAddress;
			return this;
		}

		public Builder setNumberDispatcherResourceManagerComponents(int numberDispatcherResourceManagerComponents) {
			this.numberDispatcherResourceManagerComponents = numberDispatcherResourceManagerComponents;
			return this;
		}

		public Builder setLocalCommunication(boolean localCommunication) {
			this.localCommunication = localCommunication;
			return this;
		}

		public TestingMiniClusterConfiguration build() {
			final Configuration modifiedConfiguration = new Configuration(configuration);
			modifiedConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlotsPerTaskManager);
			modifiedConfiguration.setString(
				RestOptions.ADDRESS,
				modifiedConfiguration.getString(RestOptions.ADDRESS, "localhost"));
			modifiedConfiguration.setInteger(
				RestOptions.PORT,
				modifiedConfiguration.getInteger(RestOptions.PORT, 0));

			if (!modifiedConfiguration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY) &&
				!modifiedConfiguration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY_PROCESS)) {
				modifiedConfiguration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY,
					String.valueOf(EnvironmentInformation.getMaxJvmHeapMemory() >> 20) + "m"); // bytes to megabytes
			}
			final TaskManagerResource tmResource = TaskManagerResource.calculateFromConfiguration(modifiedConfiguration);
			modifiedConfiguration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP, tmResource.getHeapMemoryMb() + "m");
			modifiedConfiguration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP_FRAMEWORK, tmResource.getFrameworkHeapMemoryMb() + "m");
			modifiedConfiguration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED, tmResource.getManagedMemoryMb() + "m");
			modifiedConfiguration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED_OFFHEAP, String.valueOf(tmResource.isManagedMemoryOffheap()));
			modifiedConfiguration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_SIZE_KEY, tmResource.getNetworkMemoryMb() + "m");
			modifiedConfiguration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_DIRECT, tmResource.getReservedDirectMemoryMb() + "m");
			modifiedConfiguration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_NATIVE, tmResource.getReservedNativeMemoryMb() + "m");

			return new TestingMiniClusterConfiguration(
				modifiedConfiguration,
				numTaskManagers,
				rpcServiceSharing,
				commonBindAddress,
				numberDispatcherResourceManagerComponents,
				localCommunication);
		}
	}
}
