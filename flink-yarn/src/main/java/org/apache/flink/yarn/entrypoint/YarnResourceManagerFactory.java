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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ActiveResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.yarn.YarnResourceManager;
import org.apache.flink.yarn.YarnWorkerNode;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * {@link ResourceManagerFactory} implementation which creates a {@link YarnResourceManager}.
 */
public class YarnResourceManagerFactory extends ActiveResourceManagerFactory<YarnWorkerNode> {

	private static final Logger LOG = LoggerFactory.getLogger(YarnResourceManagerFactory.class);

	private static final YarnResourceManagerFactory INSTANCE = new YarnResourceManagerFactory();

	private YarnResourceManagerFactory() {}

	public static YarnResourceManagerFactory getInstance() {
		return INSTANCE;
	}

	@Override
	public ResourceManager<YarnWorkerNode> createActiveResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl,
			ResourceManagerMetricGroup resourceManagerMetricGroup) throws Exception {
		final ResourceManagerRuntimeServicesConfiguration rmServicesConfiguration =
			ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration, createDefaultWorkerResourceSpec(configuration));
		final ResourceManagerRuntimeServices rmRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			rmServicesConfiguration,
			highAvailabilityServices,
			rpcService.getScheduledExecutor());

		return new YarnResourceManager(
			rpcService,
			getEndpointId(),
			resourceId,
			configuration,
			System.getenv(),
			highAvailabilityServices,
			heartbeatServices,
			rmRuntimeServices.getSlotManager(),
			rmRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler,
			webInterfaceUrl,
			resourceManagerMetricGroup);
	}

	@Override
	protected CPUResource getDefaultCpus(final Configuration configuration) {
		int fallback = configuration.getInteger(YarnConfigOptions.VCORES);
		double cpuCoresDouble = TaskExecutorProcessUtils.getCpuCoresWithFallback(configuration, fallback).getValue().doubleValue();
		@SuppressWarnings("NumericCastThatLosesPrecision")
		long cpuCoresLong = Math.max((long) Math.ceil(cpuCoresDouble), 1L);
		//noinspection FloatingPointEquality
		if (cpuCoresLong != cpuCoresDouble) {
			LOG.info(
				"The amount of cpu cores must be a positive integer on Yarn. Rounding {} up to the closest positive integer {}.",
				cpuCoresDouble,
				cpuCoresLong);
		}
		if (cpuCoresLong > Integer.MAX_VALUE) {
			throw new IllegalConfigurationException(String.format(
				"The amount of cpu cores %d cannot exceed Integer.MAX_VALUE: %d",
				cpuCoresLong,
				Integer.MAX_VALUE));
		}
		//noinspection NumericCastThatLosesPrecision
		return new CPUResource(cpuCoresLong);
	}
}
