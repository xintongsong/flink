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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * The set of configuration options relating to TaskManager and Task settings.
 */
@PublicEvolving
@ConfigGroups(groups = @ConfigGroup(name = "TaskManagerMemory", keyPrefix = "taskmanager.memory"))
public class TaskManagerOptions {

	// ------------------------------------------------------------------------
	//  General TaskManager Options
	// ------------------------------------------------------------------------

	/**
	 * Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.
	 */
	public static final ConfigOption<Boolean> KILL_ON_OUT_OF_MEMORY =
			key("taskmanager.jvm-exit-on-oom")
			.defaultValue(false)
			.withDescription("Whether to kill the TaskManager when the task thread throws an OutOfMemoryError.");

	/**
	 * Whether the quarantine monitor for task managers shall be started. The quarantine monitor
	 * shuts down the actor system if it detects that it has quarantined another actor system
	 * or if it has been quarantined by another actor system.
	 */
	public static final ConfigOption<Boolean> EXIT_ON_FATAL_AKKA_ERROR =
			key("taskmanager.exit-on-fatal-akka-error")
			.defaultValue(false)
			.withDescription("Whether the quarantine monitor for task managers shall be started. The quarantine monitor" +
				" shuts down the actor system if it detects that it has quarantined another actor system" +
				" or if it has been quarantined by another actor system.");

	/**
	 * The config parameter defining the task manager's hostname.
	 * Overrides {@link #HOST_BIND_POLICY} automatic address binding.
	 */
	public static final ConfigOption<String> HOST =
		key("taskmanager.host")
			.noDefaultValue()
			.withDescription("The address of the network interface that the TaskManager binds to." +
				" This option can be used to define explicitly a binding address. Because" +
				" different TaskManagers need different values for this option, usually it is specified in an" +
				" additional non-shared TaskManager-specific config file.");

	/**
	 * The config parameter for automatically defining the TaskManager's binding address,
	 * if {@link #HOST} configuration option is not set.
	 */
	public static final ConfigOption<String> HOST_BIND_POLICY =
		key("taskmanager.network.bind-policy")
			.defaultValue("ip")
			.withDescription(Description.builder()
				.text("The automatic address binding policy used by the TaskManager if \"" + HOST.key() + "\" is not set." +
					" The value should be one of the following:\n")
				.list(
					text("\"name\" - uses hostname as binding address"),
					text("\"ip\" - uses host's ip address as binding address"))
				.build());

	/**
	 * The default network port range the task manager expects incoming IPC connections. The {@code "0"} means that
	 * the TaskManager searches for a free port.
	 */
	public static final ConfigOption<String> RPC_PORT =
		key("taskmanager.rpc.port")
			.defaultValue("0")
			.withDescription("The task manager’s IPC port. Accepts a list of ports (“50100,50101”), ranges" +
				" (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid" +
				" collisions when multiple TaskManagers are running on the same machine.");

	/**
	 * The initial registration backoff between two consecutive registration attempts. The backoff
	 * is doubled for each new registration attempt until it reaches the maximum registration backoff.
	 */
	public static final ConfigOption<String> INITIAL_REGISTRATION_BACKOFF =
		key("taskmanager.registration.initial-backoff")
			.defaultValue("500 ms")
			.withDeprecatedKeys("taskmanager.initial-registration-pause")
			.withDescription("The initial registration backoff between two consecutive registration attempts. The backoff" +
				" is doubled for each new registration attempt until it reaches the maximum registration backoff.");

	/**
	 * The maximum registration backoff between two consecutive registration attempts.
	 */
	public static final ConfigOption<String> REGISTRATION_MAX_BACKOFF =
		key("taskmanager.registration.max-backoff")
			.defaultValue("30 s")
			.withDeprecatedKeys("taskmanager.max-registration-pause")
			.withDescription("The maximum registration backoff between two consecutive registration attempts. The max" +
				" registration backoff requires a time unit specifier (ms/s/min/h/d).");

	/**
	 * The backoff after a registration has been refused by the job manager before retrying to connect.
	 */
	public static final ConfigOption<String> REFUSED_REGISTRATION_BACKOFF =
		key("taskmanager.registration.refused-backoff")
			.defaultValue("10 s")
			.withDeprecatedKeys("taskmanager.refused-registration-pause")
			.withDescription("The backoff after a registration has been refused by the job manager before retrying to connect.");

	/**
	 * Defines the timeout it can take for the TaskManager registration. If the duration is
	 * exceeded without a successful registration, then the TaskManager terminates.
	 */
	public static final ConfigOption<String> REGISTRATION_TIMEOUT =
		key("taskmanager.registration.timeout")
			.defaultValue("5 min")
			.withDeprecatedKeys("taskmanager.maxRegistrationDuration")
			.withDescription("Defines the timeout for the TaskManager registration. If the duration is" +
				" exceeded without a successful registration, then the TaskManager terminates.");

	/**
	 * The config parameter defining the number of task slots of a task manager.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_PARALLELISM_SLOTS)
	public static final ConfigOption<Integer> NUM_TASK_SLOTS =
		key("taskmanager.numberOfTaskSlots")
			.defaultValue(1)
			.withDescription("The number of parallel operator or user function instances that a single TaskManager can" +
				" run. If this value is larger than 1, a single TaskManager takes multiple instances of a function or" +
				" operator. That way, the TaskManager can utilize multiple CPU cores, but at the same time, the" +
				" available memory is divided between the different operator or function instances. This value" +
				" is typically proportional to the number of physical CPU cores that the TaskManager's machine has" +
				" (e.g., equal to the number of cores, or half the number of cores).");

	public static final ConfigOption<Boolean> DEBUG_MEMORY_LOG =
		key("taskmanager.debug.memory.log")
			.defaultValue(false)
			.withDeprecatedKeys("taskmanager.debug.memory.startLogThread")
			.withDescription("Flag indicating whether to start a thread, which repeatedly logs the memory usage of the JVM.");

	public static final ConfigOption<Long> DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS =
		key("taskmanager.debug.memory.log-interval")
			.defaultValue(5000L)
			.withDeprecatedKeys("taskmanager.debug.memory.logIntervalMs")
			.withDescription("The interval (in ms) for the log thread to log the current memory usage.");

	// ------------------------------------------------------------------------
	//  Memory Options
	// ------------------------------------------------------------------------

	/**
	 * Total Flink memory size for the TaskManagers.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_MEMORY)
	public static final ConfigOption<String> TASK_MANAGER_MEMORY =
		key("taskmanager.memory.size")
			.noDefaultValue()
			.withDescription("Total Flink memory size for the TaskManagers, which are the parallel workers of the system."
				+ " This includes JVM heap memory, managed memory and network memory. This excludes JVM metaspace,"
				+ " other JVM overhead, and user-allocated direct and native memory. On YARN setups, this value is"
				+ " automatically configured to the size of the TaskManager's YARN container, minus the excluded"
				+ " portions.");

	/**
	 * Total process memory size for the TaskManagers.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_PROCESS =
		key("taskmanager.memory.process")
			.noDefaultValue()
			.withDescription("Total process memory size for the TaskManagers, which are the parallel workers of the system."
				+ " This includes JVM heap memory, managed memory, network memory, JVM metaspace, other JVM overhead,"
				+ " and user-allocated direct and native memory. On YARN setups, this value is automatically configured"
				+ " to the size of the TaskManager's YARN container.");

	/**
	 * JVM heap memory size for Flink and user code for the TaskManagers.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_HEAP =
		key("taskmanager.memory.heap")
			.noDefaultValue()
			.withDescription("JVM heap memory size for Flink and user code for the TaskManagers, which does not include"
				+ " on heap managed memory. If not specified, it will be derived as the total Flink memory (specified"
				+ " by taskmanager.memory.size) minus managed memory (specified by"
				+ " taskmanager.memory.managed.[size|fraction]) and network memory (specified by"
				+ " taskmanager.memory.network.[min|max|fraction]).");

	/**
	 * JVM heap size (in megabytes) for the TaskManagers.
	 *
	 * @deprecated use {@link #TASK_MANAGER_MEMORY} for total Flink memory, and {@link #TASK_MANAGER_MEMORY_HEAP} for
	 * heap memory
	 */
	@Deprecated
	public static final ConfigOption<Integer> TASK_MANAGER_HEAP_MEMORY_MB =
		key("taskmanager.heap.mb")
			.defaultValue(1024)
			.withDescription("JVM heap size (in megabytes) for the TaskManagers, which are the parallel workers of" +
				" the system. On YARN setups, this value is automatically configured to the size of the TaskManager's" +
				" YARN container, minus a certain tolerance value.");

	/**
	 * JVM heap memory size for Flink for the TaskManagers.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_HEAP_FRAMEWORK =
		key("taskmanager.memory.heap.framework")
			.defaultValue("128m")
			.withDescription("JVM heap memory size for Flink for the TaskManagers, which is the portion of heap memory"
				+ "used by Flink framework only.");

	/**
	 * Managed memory size for the TaskManagers.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_MANAGED =
		key("taskmanager.memory.managed.size")
			.noDefaultValue()
			.withDescription("Managed memory size for the TaskManagers, which is the amount of memory reserved for"
				+ " sorting, hash tables, caching of intermediate results and state backends. Memory consumers can"
				+ " either allocate memory from the memory manager in the form of MemorySegments, or reserve bytes from"
				+ " the memory manager and keep their memory usage within that boundary. If unspecified, it will be"
				+ " derived as a relative fraction (specified by taskmanager.memory.managed.fraction) of the total Flink"
				+ " memory (specified by taskmanager.memory.size).");

	/**
	 * Fraction of total Flink memory to use as managed memory, if {@link #TASK_MANAGER_MEMORY_MANAGED} is not set.
	 */
	public static final ConfigOption<Float> TASK_MANAGER_MEMORY_MANAGED_FRACTION =
		key("taskmanager.memory.managed.fraction")
			.defaultValue(0.5f)
			.withDeprecatedKeys("taskmanager.memory.fraction")
			.withDescription("The relative amount of total Flink memory that is used as managed memory, which is the"
				+ " amount of memory reserved for sorting, hash tables, caching of intermediate results and state"
				+ " backends. Memory consumers can either allocate memory from the memory manager in the form of"
				+ " MemorySegments, or reserve bytes from the memory manager and keep their memory usage within that"
				+ " boundary. This parameter is only evaluated if \"taskmanager.memory.managed.size\" is not set.");

	/**
	 * Memory allocation method (JVM heap or off-heap), used for managed memory of the TaskManagers.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_MANAGED_OFFHEAP =
		key("taskmanager.memory.managed.off-heap")
			.defaultValue("auto")
			.withDeprecatedKeys("taskmanager.memory.off-heap")
			.withDescription("Memory allocation method (JVM heap or off-heap), used for managed memory of the"
				+ " TaskManagers. For setups with larger quantities of memory, this can improve the efficiency of the"
				+ " operations performed on the memory. Valid values of this config option are \"true\", \"false\", and"
				+ " \"auto\". When set to auto, Flink will decide the allocation method depending on the configured"
				+ " state backend (heap for MemoryStateBackend and FsStateBackend, off-heap for RocksDBStateBackend and"
				+ " no state backend). ");

	/**
	 * Fraction of total Flink memory to use for network buffers.
	 */
	public static final ConfigOption<Float> TASK_MANAGER_MEMORY_NETWORK_FRACTION =
		key("taskmanager.memory.network.fraction")
			.defaultValue(0.1f)
			.withFallbackKeys("taskmanager.network.memory.fraction")
			.withDescription("Fraction of total Flink memory to use for network buffers. This determines how many"
				+ " streaming data exchange channels a TaskManager can have at the same time and how well buffered the"
				+ " channels are. If a job is rejected or you get a warning that the system has not enough buffers"
				+ " available, increase this value or the min/max values below. Also note, that "
				+ "\"taskmanager.memory.network.min\" and \"taskmanager.memory.network.max\" may override this fraction.");

	/**
	 * Minimum memory size for network buffers.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_NETWORK_MIN =
		key("taskmanager.memory.network.min")
			.defaultValue("64mb")
			.withFallbackKeys("taskmanager.network.memory.min")
			.withDescription("Minimum memory size for network buffers.");

	/**
	 * Maximum memory size for network buffers.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_NETWORK_MAX =
		key("taskmanager.memory.network.max")
			.defaultValue("1gb")
			.withFallbackKeys("taskmanager.network.memory.max")
			.withDescription("Maximum memory size for network buffers.");

	/**
	 * Configuration key of memory size for network buffers.
	 * This is only used to store memory size derived from {@link #TASK_MANAGER_MEMORY_NETWORK_MIN},
	 * {@link #TASK_MANAGER_MEMORY_NETWORK_MAX} and {@link #TASK_MANAGER_MEMORY_NETWORK_FRACTION} in the
	 * {@link Configuration}. It should not be exposed to the users.
	 */
	public static final String TASK_MANAGER_MEMORY_NETWORK_SIZE_KEY = "taskmanager.memory.network";

	/**
	 * JVM metaspace memory size for the TaskManagers.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_JVM_METASPACE =
		key("taskmanager.memory.jvm-metaspace")
			.defaultValue("192m")
			.withDescription("JVM metaspace memory size for the TaskManagers.");

	/**
	 * Fraction of total process memory reserved for JVM overheads.
	 */
	public static final ConfigOption<Float> TASK_MANAGER_MEMORY_JVM_OVERHEAD_FRACTION =
		key("taskmanager.memory.jvm-overhead.fraction")
			.defaultValue(0.1f)
			.withDescription("Fraction of total process memory reserved for JVM overheads, which are thread stack space,"
				+ " I/O direct memory, compile cache, etc. Note, that \"taskmanager.memory.jvm-overhead.min\" and"
				+ " \"taskmanager.memory.jvm-overhead.max\" may override this fraction.");

	/**
	 * Minimum memory size reserved for JVM overheads.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_JVM_OVERHEAD_MIN =
		key("taskmanager.memory.jvm-overhead.min")
			.defaultValue("128m")
			.withDescription("Minimum memory size reserved for JVM overheads.");

	/**
	 * Maximum memory size reserved for JVM overheads.
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_JVM_OVERHEAD_MAX =
		key("taskmanager.memory.jvm-overhead.max")
			.defaultValue("1g")
			.withDescription("Maximum memory size reserved for JVM overheads.");

	/**
	 * Memory size reserved for user-allocated direct memory (for libraries).
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_RESERVED_DIRECT =
		key("taskmanager.memory.reserved.direct")
			.defaultValue("0b")
			.withDescription("Memory size reserved for user-allocated direct memory (for libraries).");

	/**
	 * Memory size reserved for user-allocated native memory (for libraries).
	 */
	public static final ConfigOption<String> TASK_MANAGER_MEMORY_RESERVED_NATIVE =
		key("taskmanager.memory.reserved.native")
			.defaultValue("0b")
			.withDescription("Memory size reserved for user-allocated native memory (for libraries).");

	/**
	 * Size of memory buffers used by the network stack and the memory manager.
	 */
	public static final ConfigOption<String> MEMORY_SEGMENT_SIZE =
			key("taskmanager.memory.segment-size")
			.defaultValue("32kb")
			.withDescription("Size of memory buffers used by the network stack and the memory manager.");

	// ------------------------------------------------------------------------
	//  Task Options
	// ------------------------------------------------------------------------

	/**
	 * Time interval in milliseconds between two successive task cancellation
	 * attempts.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_INTERVAL =
			key("task.cancellation.interval")
			.defaultValue(30000L)
			.withDeprecatedKeys("task.cancellation-interval")
			.withDescription("Time interval between two successive task cancellation attempts in milliseconds.");

	/**
	 * Timeout in milliseconds after which a task cancellation times out and
	 * leads to a fatal TaskManager error. A value of <code>0</code> deactivates
	 * the watch dog.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_TIMEOUT =
			key("task.cancellation.timeout")
			.defaultValue(180000L)
			.withDescription("Timeout in milliseconds after which a task cancellation times out and" +
				" leads to a fatal TaskManager error. A value of 0 deactivates" +
				" the watch dog.");
	/**
	 * This configures how long we wait for the timers in milliseconds to finish all pending timer threads
	 * when the stream task is cancelled.
	 */
	public static final ConfigOption<Long> TASK_CANCELLATION_TIMEOUT_TIMERS = ConfigOptions
			.key("task.cancellation.timers.timeout")
			.defaultValue(7500L)
			.withDeprecatedKeys("timerservice.exceptional.shutdown.timeout")
			.withDescription("Time we wait for the timers in milliseconds to finish all pending timer threads" +
				" when the stream task is cancelled.");

	/**
	 * The maximum number of bytes that a checkpoint alignment may buffer.
	 * If the checkpoint alignment buffers more than the configured amount of
	 * data, the checkpoint is aborted (skipped).
	 *
	 * <p>The default value of {@code -1} indicates that there is no limit.
	 */
	public static final ConfigOption<Long> TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT =
			key("task.checkpoint.alignment.max-size")
			.defaultValue(-1L)
			.withDescription("The maximum number of bytes that a checkpoint alignment may buffer. If the checkpoint" +
				" alignment buffers more than the configured amount of data, the checkpoint is aborted (skipped)." +
				" A value of -1 indicates that there is no limit.");

	// ------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private TaskManagerOptions() {}
}
