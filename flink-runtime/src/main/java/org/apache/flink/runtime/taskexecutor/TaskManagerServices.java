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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Container for {@link TaskExecutor} services such as the {@link MemoryManager}, {@link IOManager},
 * {@link NetworkEnvironment}. All services are exclusive to a single {@link TaskExecutor}.
 * Consequently, the respective {@link TaskExecutor} is responsible for closing them.
 */
public class TaskManagerServices {
	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerServices.class);

	@VisibleForTesting
	public static final String LOCAL_STATE_SUB_DIRECTORY_ROOT = "localState";

	/** TaskManager services. */
	private final TaskManagerLocation taskManagerLocation;
	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final NetworkEnvironment networkEnvironment;
	private final KvStateService kvStateService;
	private final BroadcastVariableManager broadcastVariableManager;
	private final TaskSlotTable taskSlotTable;
	private final JobManagerTable jobManagerTable;
	private final JobLeaderService jobLeaderService;
	private final TaskExecutorLocalStateStoresManager taskManagerStateStore;
	private final TaskEventDispatcher taskEventDispatcher;

	TaskManagerServices(
		TaskManagerLocation taskManagerLocation,
		MemoryManager memoryManager,
		IOManager ioManager,
		NetworkEnvironment networkEnvironment,
		KvStateService kvStateService,
		BroadcastVariableManager broadcastVariableManager,
		TaskSlotTable taskSlotTable,
		JobManagerTable jobManagerTable,
		JobLeaderService jobLeaderService,
		TaskExecutorLocalStateStoresManager taskManagerStateStore,
		TaskEventDispatcher taskEventDispatcher) {

		this.taskManagerLocation = Preconditions.checkNotNull(taskManagerLocation);
		this.memoryManager = Preconditions.checkNotNull(memoryManager);
		this.ioManager = Preconditions.checkNotNull(ioManager);
		this.networkEnvironment = Preconditions.checkNotNull(networkEnvironment);
		this.kvStateService = Preconditions.checkNotNull(kvStateService);
		this.broadcastVariableManager = Preconditions.checkNotNull(broadcastVariableManager);
		this.taskSlotTable = Preconditions.checkNotNull(taskSlotTable);
		this.jobManagerTable = Preconditions.checkNotNull(jobManagerTable);
		this.jobLeaderService = Preconditions.checkNotNull(jobLeaderService);
		this.taskManagerStateStore = Preconditions.checkNotNull(taskManagerStateStore);
		this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	public MemoryManager getMemoryManager() {
		return memoryManager;
	}

	public IOManager getIOManager() {
		return ioManager;
	}

	public NetworkEnvironment getNetworkEnvironment() {
		return networkEnvironment;
	}

	public KvStateService getKvStateService() {
		return kvStateService;
	}

	public TaskManagerLocation getTaskManagerLocation() {
		return taskManagerLocation;
	}

	public BroadcastVariableManager getBroadcastVariableManager() {
		return broadcastVariableManager;
	}

	public TaskSlotTable getTaskSlotTable() {
		return taskSlotTable;
	}

	public JobManagerTable getJobManagerTable() {
		return jobManagerTable;
	}

	public JobLeaderService getJobLeaderService() {
		return jobLeaderService;
	}

	public TaskExecutorLocalStateStoresManager getTaskManagerStateStore() {
		return taskManagerStateStore;
	}

	public TaskEventDispatcher getTaskEventDispatcher() {
		return taskEventDispatcher;
	}

	// --------------------------------------------------------------------------------------------
	//  Shut down method
	// --------------------------------------------------------------------------------------------

	/**
	 * Shuts the {@link TaskExecutor} services down.
	 */
	public void shutDown() throws FlinkException {

		Exception exception = null;

		try {
			taskManagerStateStore.shutdown();
		} catch (Exception e) {
			exception = e;
		}

		try {
			memoryManager.shutdown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			ioManager.shutdown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			networkEnvironment.shutdown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			kvStateService.shutdown();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			taskSlotTable.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			jobLeaderService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		taskEventDispatcher.clearAll();

		if (exception != null) {
			throw new FlinkException("Could not properly shut down the TaskManager services.", exception);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Static factory methods for task manager services
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates and returns the task manager services.
	 *
	 * @param taskManagerServicesConfiguration task manager configuration
	 * @param taskManagerMetricGroup metric group of the task manager
	 * @param resourceID resource ID of the task manager
	 * @param taskIOExecutor executor for async IO operations
	 * @return task manager components
	 * @throws Exception
	 */
	public static TaskManagerServices fromConfiguration(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration,
			MetricGroup taskManagerMetricGroup,
			ResourceID resourceID,
			Executor taskIOExecutor) throws Exception {

		// pre-start checks
		checkTempDirs(taskManagerServicesConfiguration.getTmpDirPaths());

		final TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();

		// start the I/O manager, it will create some temp directories.
		final IOManager ioManager = new IOManagerAsync(taskManagerServicesConfiguration.getTmpDirPaths());

		final NetworkEnvironment network = NetworkEnvironment.create(
			taskManagerServicesConfiguration.getNetworkConfig(), taskEventDispatcher, taskManagerMetricGroup, ioManager);
		network.start();

		final KvStateService kvStateService = KvStateService.fromConfiguration(taskManagerServicesConfiguration);
		kvStateService.start();

		final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(
			resourceID,
			taskManagerServicesConfiguration.getTaskManagerAddress(),
			network.getConnectionManager().getDataPort());

		// this call has to happen strictly after the network stack has been initialized
		final MemoryManager memoryManager = createMemoryManager(taskManagerServicesConfiguration);

		final BroadcastVariableManager broadcastVariableManager = new BroadcastVariableManager();

		final List<ResourceProfile> resourceProfiles = new ArrayList<>(taskManagerServicesConfiguration.getNumberOfSlots());

		for (int i = 0; i < taskManagerServicesConfiguration.getNumberOfSlots(); i++) {
			resourceProfiles.add(ResourceProfile.ANY);
		}

		final TimerService<AllocationID> timerService = new TimerService<>(
			new ScheduledThreadPoolExecutor(1),
			taskManagerServicesConfiguration.getTimerServiceShutdownTimeout());

		final TaskSlotTable taskSlotTable = new TaskSlotTable(resourceProfiles, timerService);

		final JobManagerTable jobManagerTable = new JobManagerTable();

		final JobLeaderService jobLeaderService = new JobLeaderService(taskManagerLocation, taskManagerServicesConfiguration.getRetryingRegistrationConfiguration());

		final String[] stateRootDirectoryStrings = taskManagerServicesConfiguration.getLocalRecoveryStateRootDirectories();

		final File[] stateRootDirectoryFiles = new File[stateRootDirectoryStrings.length];

		for (int i = 0; i < stateRootDirectoryStrings.length; ++i) {
			stateRootDirectoryFiles[i] = new File(stateRootDirectoryStrings[i], LOCAL_STATE_SUB_DIRECTORY_ROOT);
		}

		final TaskExecutorLocalStateStoresManager taskStateManager = new TaskExecutorLocalStateStoresManager(
			taskManagerServicesConfiguration.isLocalRecoveryEnabled(),
			stateRootDirectoryFiles,
			taskIOExecutor);

		return new TaskManagerServices(
			taskManagerLocation,
			memoryManager,
			ioManager,
			network,
			kvStateService,
			broadcastVariableManager,
			taskSlotTable,
			jobManagerTable,
			jobLeaderService,
			taskStateManager,
			taskEventDispatcher);
	}

	/**
	 * Creates a {@link MemoryManager} from the given {@link TaskManagerServicesConfiguration}.
	 *
	 * @param taskManagerServicesConfiguration to create the memory manager from
	 * @return Memory manager
	 * @throws Exception
	 */
	private static MemoryManager createMemoryManager(
			TaskManagerServicesConfiguration taskManagerServicesConfiguration) throws Exception {
		long memorySizeMb = taskManagerServicesConfiguration.getTmResource().getManagedMemoryMb();
		final long memorySize = memorySizeMb << 20; // megabytes to bytes
		MemoryType memType = taskManagerServicesConfiguration.getTmResource().getManagedMemoryType();

		final MemoryManager memoryManager;
		try {
			memoryManager = new MemoryManager(
				memorySize,
				taskManagerServicesConfiguration.getNumberOfSlots(),
				taskManagerServicesConfiguration.getNetworkConfig().networkBufferSize(),
				memType);
		} catch (OutOfMemoryError e) {
			if (memType == MemoryType.HEAP) {
				throw new Exception("OutOfMemory error (" + e.getMessage() +
					") while allocating the TaskManager heap memory (" + memorySize + " bytes).", e);
			} else if (memType == MemoryType.OFF_HEAP) {
				throw new Exception("OutOfMemory error (" + e.getMessage() +
					") while allocating the TaskManager off-heap memory (" + memorySize +
					" bytes).Try increasing the maximum direct memory (-XX:MaxDirectMemorySize)", e);
			} else {
				throw e;
			}
		}
		return memoryManager;
	}

	/**
	 * Validates that all the directories denoted by the strings do actually exist or can be created, are proper
	 * directories (not files), and are writable.
	 *
	 * @param tmpDirs The array of directory paths to check.
	 * @throws IOException Thrown if any of the directories does not exist and cannot be created or is not writable
	 *                     or is a file, rather than a directory.
	 */
	private static void checkTempDirs(String[] tmpDirs) throws IOException {
		for (String dir : tmpDirs) {
			if (dir != null && !dir.equals("")) {
				File file = new File(dir);
				if (!file.exists()) {
					if (!file.mkdirs()) {
						throw new IOException("Temporary file directory " + file.getAbsolutePath() + " does not exist and could not be created.");
					}
				}
				if (!file.isDirectory()) {
					throw new IOException("Temporary file directory " + file.getAbsolutePath() + " is not a directory.");
				}
				if (!file.canWrite()) {
					throw new IOException("Temporary file directory " + file.getAbsolutePath() + " is not writable.");
				}

				if (LOG.isInfoEnabled()) {
					long totalSpaceGb = file.getTotalSpace() >> 30;
					long usableSpaceGb = file.getUsableSpace() >> 30;
					double usablePercentage = (double) usableSpaceGb / totalSpaceGb * 100;
					String path = file.getAbsolutePath();
					LOG.info(String.format("Temporary file directory '%s': total %d GB, " + "usable %d GB (%.2f%% usable)",
						path, totalSpaceGb, usableSpaceGb, usablePercentage));
				}
			} else {
				throw new IllegalArgumentException("Temporary file directory #$id is null.");
			}
		}
	}
}
