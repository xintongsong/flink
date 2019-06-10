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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.runtime.state.StateBackendLoader.FS_STATE_BACKEND_NAME;
import static org.apache.flink.runtime.state.StateBackendLoader.MEMORY_STATE_BACKEND_NAME;

/**
 * Describe the resource of a task manager.
 */
public class TaskManagerResource {
	private final double cpuCores;
	private final int heapMemoryMb;
	private final int frameworkHeapMemoryMb;
	private final int managedMemoryMb;
	private final boolean managedMemoryOffheap;
	private final int networkMemoryMb;
	private final int reservedDirectMemoryMb;
	private final int reservedNativeMemoryMb;
	private final Map<String, Resource> extendedResources = new HashMap<>(1);

	private final int jvmMetaspaceMb;
	private final int jvmOverheadMb;

	public TaskManagerResource(
		double cpuCores,
		int heapMemoryMb,
		int frameworkHeapMemoryMb,
		int managedMemoryMb,
		boolean managedMemoryOffheap,
		int networkMemoryMb,
		int reservedDirectMemoryMb,
		int reservedNativeMemoryMb,
		Map<String, Resource> extendedResources,
		int jvmMetaspaceMb,
		int jvmOverheadMb) {

		this.cpuCores = cpuCores;
		this.heapMemoryMb = heapMemoryMb;
		this.frameworkHeapMemoryMb = frameworkHeapMemoryMb;
		this.managedMemoryMb = managedMemoryMb;
		this.managedMemoryOffheap = managedMemoryOffheap;
		this.networkMemoryMb = networkMemoryMb;
		this.reservedDirectMemoryMb = reservedDirectMemoryMb;
		this.reservedNativeMemoryMb = reservedNativeMemoryMb;
		if (extendedResources != null) {
			this.extendedResources.putAll(extendedResources);
		}
		this.jvmMetaspaceMb = jvmMetaspaceMb;
		this.jvmOverheadMb = jvmOverheadMb;
	}

	public static TaskManagerResource calculateFromConfiguration(Configuration configuration) {
		double cpuCores;
		int heapMemoryMb;
		int frameworkHeapMemoryMb;
		int managedMemoryMb;
		boolean managedMemoryOffheap;
		int networkMemoryMb;
		int reservedDirectMemoryMb;
		int reservedNativeMemoryMb;
		int jvmMetaspaceMb;
		int jvmOverheadMb;

		// for now, we do not consider management of TaskManager's cpu cores
		cpuCores = -1.0;

		// parse and check configurations that are not derived from other config options

		frameworkHeapMemoryMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP_FRAMEWORK)).getMebiBytes();
		jvmMetaspaceMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_METASPACE)).getMebiBytes();
		reservedDirectMemoryMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_DIRECT)).getMebiBytes();
		reservedNativeMemoryMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_NATIVE)).getMebiBytes();

		Preconditions.checkArgument(frameworkHeapMemoryMb >= 0,
			String.format("Configured TaskManager framework heap memory size (%dm, from '%s') must be larger than or equal to 0.",
				frameworkHeapMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP_FRAMEWORK.key()));
		Preconditions.checkArgument(jvmMetaspaceMb >= 0,
			String.format("Configured TaskManager JVM metaspace memory size (%dm, from '%s') must be larger than or equal to 0.",
				jvmMetaspaceMb, TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_METASPACE.key()));
		Preconditions.checkArgument(reservedDirectMemoryMb >= 0,
			String.format("Configured TaskManager reserved direct memory size (%dm, from '%s') must be larger than or equal to 0.",
				reservedDirectMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_DIRECT.key()));
		Preconditions.checkArgument(reservedNativeMemoryMb >= 0,
			String.format("Configured TaskManager reserved native memory size (%dm, from '%s') must be larger than or equal to 0.",
				reservedNativeMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_NATIVE.key()));

		// parse and check manager memory offheap configuration

		String managedMemoryOffheapStr = configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED_OFFHEAP);
		Preconditions.checkArgument(managedMemoryOffheapStr.equalsIgnoreCase("auto") ||
				managedMemoryOffheapStr.equalsIgnoreCase("true") ||
				managedMemoryOffheapStr.equalsIgnoreCase("false"),
			String.format("Invalid value (%s) for '%s'. Valid values are: 'true', 'false' or 'auto'.",
				managedMemoryOffheapStr, TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED_OFFHEAP.key()));

		if (managedMemoryOffheapStr.equalsIgnoreCase("true")) {
			managedMemoryOffheap = true;
		} else if (managedMemoryOffheapStr.equalsIgnoreCase("false")) {
			managedMemoryOffheap = false;
		} else {
			String stateBackend = configuration.getString(CheckpointingOptions.STATE_BACKEND);
			if (stateBackend != null &&
				(stateBackend.equalsIgnoreCase(MEMORY_STATE_BACKEND_NAME) || stateBackend.equalsIgnoreCase(FS_STATE_BACKEND_NAME))) {
				managedMemoryOffheap = false;
			} else {
				managedMemoryOffheap = true;
			}
		}

		// configure heap, managed and network memory

		if ((configuration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP) ||
			configuration.contains(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB)) &&
			configuration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED)) {

			// heap memory and managed memory are configured, derive total flink memory from them

			if (configuration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP)) {
				heapMemoryMb = MemorySize.parse(
					configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP)).getMebiBytes();
			} else {
				heapMemoryMb = MemorySize.parse(
					configuration.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB) + "m").getMebiBytes();
			}
			managedMemoryMb = MemorySize.parse(
				configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED)).getMebiBytes();
			Preconditions.checkArgument(managedMemoryMb >= 0,
				String.format("Configured TaskManager managed memory size (%dm, from '%s') must be larger than or equal to 0.",
					managedMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED.key()));
			Preconditions.checkArgument(heapMemoryMb >= frameworkHeapMemoryMb,
				String.format("Configured TaskManager heap memory size (%dm, from '%s') must be larger than or equal to configured TaskManager framework heap memory size (%dm, from '%s').",
					heapMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP.key(), frameworkHeapMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP_FRAMEWORK.key()));

			networkMemoryMb = getNetworkMemoryMb(configuration, heapMemoryMb + managedMemoryMb, false);
			jvmOverheadMb = getJvmOverheadMb(configuration,
				heapMemoryMb + managedMemoryMb + networkMemoryMb + jvmMetaspaceMb + reservedDirectMemoryMb + reservedNativeMemoryMb,
				false);
		} else if (configuration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY) ||
			configuration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY_PROCESS)) {

			// configure total flink memory
			int totalFlinkMemoryMb;
			if (!configuration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY)) {
				// derive total flink memory from total process memory
				int totalProcessMemoryMb = MemorySize.parse(
					configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_PROCESS)).getMebiBytes();
				Preconditions.checkArgument(totalProcessMemoryMb > 0,
					String.format("Configured TaskManager total process memory size (%dm, from '%s') must be larger than 0.",
						totalProcessMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_PROCESS.key()));

				jvmOverheadMb = getJvmOverheadMb(configuration, totalProcessMemoryMb, true);
				totalFlinkMemoryMb = totalProcessMemoryMb - jvmMetaspaceMb - jvmOverheadMb - reservedDirectMemoryMb - reservedNativeMemoryMb;
				Preconditions.checkArgument(totalFlinkMemoryMb > 0,
					String.format("Configured TaskManager total process memory size (%dm, from '%s') must be larger than the sum of "
						+ "configured TaskManager JVM metaspace memory size (%dm, from '%s'), "
						+ "configured TaskManager JVM overhead memory size (%dm, from '%s', '%s' and '%s'), "
						+ "configured TaskManager reserved direct memory size (%dm, from '%s'), and "
						+ "configured TaskManager reserved native memory size (%dm, from '%s').",
						totalProcessMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_PROCESS.key(),
						jvmMetaspaceMb, TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_METASPACE.key(),
						jvmOverheadMb, TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_MIN.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_MAX.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_FRACTION.key(),
						reservedDirectMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_DIRECT.key(),
						reservedNativeMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_NATIVE.key()));
			} else {
				// total flink memory is configured
				totalFlinkMemoryMb = MemorySize.parse(
					configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY)).getMebiBytes();
				Preconditions.checkArgument(totalFlinkMemoryMb > 0,
					String.format("Configured TaskManager total flink memory size (%dm, from '%s') must be larger than 0.",
						totalFlinkMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY.key()));
				jvmOverheadMb = getJvmOverheadMb(configuration,
					totalFlinkMemoryMb + jvmMetaspaceMb + reservedDirectMemoryMb + reservedNativeMemoryMb,
					false);
			}

			// derive heap and managed memory from total flink memory
			networkMemoryMb = getNetworkMemoryMb(configuration, totalFlinkMemoryMb, true);
			if (configuration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED)) {
				managedMemoryMb = MemorySize.parse(
					configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED)).getMebiBytes();
				Preconditions.checkArgument(managedMemoryMb >= 0,
					String.format("Configured TaskManager managed memory size (%dm, from '%s') must be larger than or equal to 0.",
						managedMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED.key()));
			} else {
				double managedFrac = configuration.getFloat(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED_FRACTION);
				Preconditions.checkArgument(managedFrac > 0.0 && managedFrac < 1.0,
					String.format("Configured TaskManager managed memory fraction (%f, from '%s') must be larger than 0.0 and smaller than 1.0.",
						managedFrac, TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED_FRACTION.key()));
				managedMemoryMb = calculateFromFraction(0, Integer.MAX_VALUE,
					managedFrac, totalFlinkMemoryMb, true);
			}

			heapMemoryMb = totalFlinkMemoryMb - managedMemoryMb - networkMemoryMb;
			Preconditions.checkArgument(heapMemoryMb >= frameworkHeapMemoryMb,
				String.format("Derived TaskManager heap memory size (%dm) must be larger than or equal to "
						+ "configured TaskManager framework heap memory size (%dm, from '%s'). "
						+ "TaskManager heap memory size is derived from "
						+ "configured total flink memory size (%dm, from '%s', or derived from '%s', %s', %s', %s', %s', %s' and %s'), "
						+ "configured managed memory size (%dm, from '%s', or derived from '%s'), and "
						+ "configured network memory size (%dm, from '%s', '%s' and '%s').",
						heapMemoryMb, frameworkHeapMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP_FRAMEWORK.key(),
						totalFlinkMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_PROCESS.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_METASPACE.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_MIN.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_MAX.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_FRACTION.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_DIRECT.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_NATIVE.key(),
						managedMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED_FRACTION.key(),
						networkMemoryMb, TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_FRACTION.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_MIN.key(),
							TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_MAX.key()));
		} else {
			Preconditions.checkArgument(false,
				String.format("Cannot derive TaskManager memory configuration. "
					+ "Either heap memory size (from '%s') and managed memory size (from '%s'), "
					+ "or total flink memory (from '%s'), "
					+ "or total process memory (from '%s') "
					+ "need to be configured.",
					TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP.key(),
					TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED.key(),
					TaskManagerOptions.TASK_MANAGER_MEMORY.key(),
					TaskManagerOptions.TASK_MANAGER_MEMORY_PROCESS.key()));
			return null;
		}

		return new TaskManagerResource(
			cpuCores,
			heapMemoryMb,
			frameworkHeapMemoryMb,
			managedMemoryMb,
			managedMemoryOffheap,
			networkMemoryMb,
			reservedDirectMemoryMb,
			reservedNativeMemoryMb,
			Collections.emptyMap(),
			jvmMetaspaceMb,
			jvmOverheadMb);
	}

	public static TaskManagerResource directFromConfiguration(Configuration configuration) {
		double cpuCores = -1.0;
		Map<String, Resource> extendedResources = Collections.emptyMap();

		int heapMemoryMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP)).getMebiBytes();
		int frameworkHeapMemoryMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP_FRAMEWORK)).getMebiBytes();
		int managedMemoryMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED)).getMebiBytes();
		boolean managedMemoryOffheap =
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED_OFFHEAP).equalsIgnoreCase("true");
		int networkMemoryMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_SIZE_KEY, "")).getMebiBytes();
		int reservedDirectMemoryMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_DIRECT)).getMebiBytes();
		int reservedNativeMemoryMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_NATIVE)).getMebiBytes();

		int jvmMetaspaceMb = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_METASPACE)).getMebiBytes();
		int jvmOverheadMb = getJvmOverheadMb(configuration,
			heapMemoryMb + managedMemoryMb + networkMemoryMb + jvmMetaspaceMb + reservedDirectMemoryMb + reservedNativeMemoryMb,
			false);

		return new TaskManagerResource(
			cpuCores,
			heapMemoryMb,
			frameworkHeapMemoryMb,
			managedMemoryMb,
			managedMemoryOffheap,
			networkMemoryMb,
			reservedDirectMemoryMb,
			reservedNativeMemoryMb,
			extendedResources,
			jvmMetaspaceMb,
			jvmOverheadMb);
	}

	private static int getNetworkMemoryMb(Configuration configuration, int base, boolean fromTotal) {
		double frac = configuration.getFloat(TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_FRACTION);
		int min = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_MIN)).getMebiBytes();
		int max = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_MAX)).getMebiBytes();

		Preconditions.checkArgument(frac > 0.0 && frac < 1.0,
			String.format("Configured TaskManager network memory fraction (%f, from '%s') must be larger than 0.0 and smaller than 1.0.",
				frac, TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_FRACTION.key()));
		Preconditions.checkArgument(min >= 0,
			String.format("Configured TaskManager min network memory size (%dm, from '%s') must be larger than or equal to 0.",
				min, TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_MIN.key()));
		Preconditions.checkArgument(max >= min,
			String.format("Configured TaskManager max network memory size (%dm, from '%s') must be larger than or equal to configured TaskManager min network memory size (%dm, from '%s').",
				max, TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_MAX.key(), min, TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_MIN.key()));

		return calculateFromFraction(min, max, frac, base, fromTotal);
	}

	private static int getJvmOverheadMb(Configuration configuration, int base, boolean fromTotal) {
		double frac = configuration.getFloat(TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_FRACTION);
		int min = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_MIN)).getMebiBytes();
		int max = MemorySize.parse(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_MAX)).getMebiBytes();

		Preconditions.checkArgument(frac > 0.0 && frac < 1.0,
			String.format("Configured TaskManager JVM overhead memory fraction (%f, from '%s') must be larger than 0.0 and smaller than 1.0.",
				frac, TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_FRACTION.key()));
		Preconditions.checkArgument(min >= 0,
			String.format("Configured TaskManager min JVM overhead memory size (%dm, from '%s') must be larger than or equal to 0.",
			min, TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_MIN.key()));
		Preconditions.checkArgument(max >= min,
			String.format("Configured TaskManager max JVM overhead memory size (%dm, from '%s') must be larger than or equal to configured TaskManager min JVM overhead memory size (%dm, from '%s').",
				max, TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_MAX.key(), min, TaskManagerOptions.TASK_MANAGER_MEMORY_JVM_OVERHEAD_MIN.key()));

		return calculateFromFraction(min, max, frac, base, fromTotal);
	}

	private static int calculateFromFraction(int min, int max, double frac, int base, boolean fromTotal) {
		int relative = (int) (fromTotal ? base * frac : base * frac / (1 - frac));
		return Math.max(min, Math.min(max, relative));
	}

	public double getCpuCores() {
		return cpuCores;
	}

	public int getHeapMemoryMb() {
		return heapMemoryMb;
	}

	public int getFrameworkHeapMemoryMb() {
		return frameworkHeapMemoryMb;
	}

	public int getManagedMemoryMb() {
		return managedMemoryMb;
	}

	public boolean isManagedMemoryOffheap() {
		return managedMemoryOffheap;
	}

	public MemoryType getManagedMemoryType() {
		return managedMemoryOffheap ? MemoryType.OFF_HEAP : MemoryType.HEAP;
	}

	public int getNetworkMemoryMb() {
		return networkMemoryMb;
	}

	public int getReservedDirectMemoryMb() {
		return reservedDirectMemoryMb;
	}

	public int getReservedNativeMemoryMb() {
		return reservedNativeMemoryMb;
	}

	public Map<String, Resource> getExtendedResources() {
		return Collections.unmodifiableMap(extendedResources);
	}

	public int getJvmMetaspaceMb() {
		return jvmMetaspaceMb;
	}

	public int getJvmOverheadMb() {
		return jvmOverheadMb;
	}

	public int getJvmHeapMemoryMb() {
		return heapMemoryMb + (managedMemoryOffheap ? 0 : managedMemoryMb);
	}

	public int getJvmDirectMemoryMb() {
		return networkMemoryMb + reservedDirectMemoryMb + (managedMemoryOffheap ? managedMemoryMb : 0);
	}

	public int getTotalFlinkMemoryMb() {
		return heapMemoryMb + managedMemoryMb + networkMemoryMb;
	}

	public int getTotalProcessMemoryMb() {
		return heapMemoryMb + managedMemoryMb + networkMemoryMb + jvmMetaspaceMb + jvmOverheadMb + reservedDirectMemoryMb + reservedNativeMemoryMb;
	}

	@Override
	public String toString() {
		String strExtendedResource = "";
		if (!extendedResources.isEmpty()) {
			final StringBuilder resources = new StringBuilder(extendedResources.size() * 10);
			for (Map.Entry<String, Resource> resource : extendedResources.entrySet()) {
				resources.append(resource.getKey()).append('=').append(resource.getValue().getValue()).append(", ");
			}
			strExtendedResource = resources.toString();
		}

		return "TaskManagerResource {"
			+ "cpuCores=" + cpuCores + ", "
			+ "heapMemoryMb=" + heapMemoryMb + ", "
			+ "frameworkHeapMemoryMb=" + frameworkHeapMemoryMb + ", "
			+ "managedMemoryMb=" + managedMemoryMb + ", "
			+ "managedMemoryOffheap=" + managedMemoryOffheap + ", "
			+ "networkMemoryMb=" + networkMemoryMb + ", "
			+ "reservedDirectMemoryMb=" + reservedDirectMemoryMb + ", "
			+ "reservedNativeMemoryMb=" + reservedNativeMemoryMb + ", "
			+ strExtendedResource
			+ "jvmMetaspaceMb=" + jvmMetaspaceMb + ", "
			+ "jvmOverheadMb=" + jvmOverheadMb
			+ "}";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == TaskManagerResource.class) {
			TaskManagerResource that = (TaskManagerResource) obj;
			return this.cpuCores == that.cpuCores &&
				this.heapMemoryMb == that.heapMemoryMb &&
				this.frameworkHeapMemoryMb == that.frameworkHeapMemoryMb &&
				this.managedMemoryMb == that.managedMemoryMb &&
				this.managedMemoryOffheap == that.managedMemoryOffheap &&
				this.networkMemoryMb == that.networkMemoryMb &&
				this.reservedDirectMemoryMb == that.reservedDirectMemoryMb &&
				this.reservedNativeMemoryMb == that.reservedNativeMemoryMb &&
				this.extendedResources.equals(that.extendedResources) &&
				this.jvmMetaspaceMb == that.jvmMetaspaceMb &&
				this.jvmOverheadMb == that.jvmOverheadMb;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(cpuCores, heapMemoryMb, frameworkHeapMemoryMb, managedMemoryMb, managedMemoryOffheap,
			networkMemoryMb, reservedDirectMemoryMb, reservedNativeMemoryMb, extendedResources, jvmMetaspaceMb, jvmOverheadMb);
	}
}
