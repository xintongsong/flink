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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.TaskManagerResource;
import org.apache.flink.runtime.testutils.SystemExitTrackingSecurityManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link TaskManagerRunner}.
 */
public class TaskManagerRunnerTest extends TestLogger {

	@Rule
	public final Timeout timeout = Timeout.seconds(30);

	private SystemExitTrackingSecurityManager systemExitTrackingSecurityManager;
	private TaskManagerRunner taskManagerRunner;

	@Before
	public void before() {
		systemExitTrackingSecurityManager = new SystemExitTrackingSecurityManager();
		System.setSecurityManager(systemExitTrackingSecurityManager);
	}

	@After
	public void after() throws Exception {
		System.setSecurityManager(null);
		if (taskManagerRunner != null) {
			taskManagerRunner.close();
		}
	}

	@Test
	public void testShouldShutdownOnFatalError() throws Exception {
		Configuration configuration = createConfiguration();
		// very high timeout, to ensure that we don't fail because of registration timeouts
		configuration.setString(TaskManagerOptions.REGISTRATION_TIMEOUT, "42 h");
		taskManagerRunner = createTaskManagerRunner(configuration);

		taskManagerRunner.onFatalError(new RuntimeException());

		Integer statusCode = systemExitTrackingSecurityManager.getSystemExitFuture().get();
		assertThat(statusCode, is(equalTo(TaskManagerRunner.RUNTIME_FAILURE_RETURN_CODE)));
	}

	@Test
	public void testShouldShutdownIfRegistrationWithJobManagerFails() throws Exception {
		Configuration configuration = createConfiguration();
		configuration.setString(TaskManagerOptions.REGISTRATION_TIMEOUT, "10 ms");
		taskManagerRunner = createTaskManagerRunner(configuration);

		Integer statusCode = systemExitTrackingSecurityManager.getSystemExitFuture().get();
		assertThat(statusCode, is(equalTo(TaskManagerRunner.RUNTIME_FAILURE_RETURN_CODE)));
	}

	private static Configuration createConfiguration() {
		final Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.ADDRESS, "localhost");
		configuration.setString(TaskManagerOptions.HOST, "localhost");

		if (!configuration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY) &&
			!configuration.contains(TaskManagerOptions.TASK_MANAGER_MEMORY_PROCESS)) {
			configuration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY,
				String.valueOf(EnvironmentInformation.getMaxJvmHeapMemory() >> 20)+"m"); // bytes to megabytes
		}
		final TaskManagerResource tmResource = TaskManagerResource.calculateFromConfiguration(configuration);
		configuration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP, tmResource.getHeapMemoryMb() + "m");
		configuration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_HEAP_FRAMEWORK, tmResource.getFrameworkHeapMemoryMb() + "m");
		configuration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED, tmResource.getManagedMemoryMb() + "m");
		configuration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_MANAGED_OFFHEAP, String.valueOf(tmResource.isManagedMemoryOffheap()));
		configuration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_SIZE_KEY, tmResource.getNetworkMemoryMb() + "m");
		configuration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_DIRECT, tmResource.getReservedDirectMemoryMb() + "m");
		configuration.setString(TaskManagerOptions.TASK_MANAGER_MEMORY_RESERVED_NATIVE, tmResource.getReservedNativeMemoryMb() + "m");
		return configuration;
	}

	private static TaskManagerRunner createTaskManagerRunner(final Configuration configuration) throws Exception {
		TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, ResourceID.generate());
		taskManagerRunner.start();
		return taskManagerRunner;
	}
}
