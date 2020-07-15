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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesResourceProvider;
import org.apache.flink.kubernetes.KubernetesWorkerNode;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerConfiguration;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.resourcemanager.active.NewActiveResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.active.ResourceProvider;
import org.apache.flink.util.ConfigurationException;

/**
 * NewKubernetesResourceManagerFactory.
 */
public class NewKubernetesResourceManagerFactory extends NewActiveResourceManagerFactory<KubernetesWorkerNode> {

	private static final NewKubernetesResourceManagerFactory INSTANCE = new NewKubernetesResourceManagerFactory();

	private static final Time POD_CREATION_RETRY_INTERVAL = Time.seconds(3L);

	private NewKubernetesResourceManagerFactory() {}

	public static NewKubernetesResourceManagerFactory getInstance() {
		return INSTANCE;
	}

	@Override
	protected ResourceProvider<KubernetesWorkerNode> createResourceProvider(Configuration configuration) {
		final KubernetesResourceManagerConfiguration kubernetesResourceManagerConfiguration =
			new KubernetesResourceManagerConfiguration(
				configuration.getString(KubernetesConfigOptions.CLUSTER_ID),
				POD_CREATION_RETRY_INTERVAL);

		return new KubernetesResourceProvider(
			configuration,
			KubeClientFactory.fromConfiguration(configuration),
			kubernetesResourceManagerConfiguration);
	}

	@Override
	protected ResourceManagerRuntimeServicesConfiguration createResourceManagerRuntimeServicesConfiguration(
		Configuration configuration) throws ConfigurationException {
		return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration, KubernetesWorkerResourceSpecFactory.INSTANCE);
	}
}
