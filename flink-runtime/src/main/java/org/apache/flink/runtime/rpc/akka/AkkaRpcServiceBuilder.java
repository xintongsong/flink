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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.net.InetAddress;

/**
 * Builder for {@link AkkaRpcService}.
 */
public class AkkaRpcServiceBuilder {

	private final Configuration configuration;
	private final Logger logger;
	@Nullable private final String externalAddress;
	@Nullable private final String externalPortRange;

	private String actorSystemName = AkkaUtils.getFlinkActorSystemName();
	@Nullable private BootstrapTools.ActorSystemExecutorConfiguration actorSystemExecutorConfiguration = null;
	@Nullable private Config customConfig = null;
	private String bindAddress = NetUtils.getWildcardIPAddress();
	private int bindPort = -1;

	/**
	 * Builder for creating a remote RPC service.
	 */
	public AkkaRpcServiceBuilder(
		final Configuration configuration,
		final Logger logger,
		@Nullable final String externalAddress,
		final String externalPortRange) {
		this.configuration = Preconditions.checkNotNull(configuration);
		this.logger = Preconditions.checkNotNull(logger);
		this.externalAddress = externalAddress == null ? InetAddress.getLoopbackAddress().getHostAddress() : externalAddress;
		this.externalPortRange = Preconditions.checkNotNull(externalPortRange);
	}

	/**
	 * Builder for creating a local RPC service.
	 */
	public AkkaRpcServiceBuilder(
			final Configuration configuration,
			final Logger logger) {
		this.configuration = Preconditions.checkNotNull(configuration);
		this.logger = Preconditions.checkNotNull(logger);
		this.externalAddress = null;
		this.externalPortRange = null;
	}

	public AkkaRpcServiceBuilder withActorSystemName(final String actorSystemName) {
		this.actorSystemName = Preconditions.checkNotNull(actorSystemName);
		return this;
	}

	public AkkaRpcServiceBuilder withActorSystemExecutorConfiguration(
			final BootstrapTools.ActorSystemExecutorConfiguration actorSystemExecutorConfiguration) {
		this.actorSystemExecutorConfiguration = actorSystemExecutorConfiguration;
		return this;
	}

	public AkkaRpcServiceBuilder withCustomConfig(final Config customConfig) {
		this.customConfig = customConfig;
		return this;
	}

	public AkkaRpcServiceBuilder withBindAddress(final String bindAddress) {
		this.bindAddress = Preconditions.checkNotNull(bindAddress);
		return this;
	}

	public AkkaRpcServiceBuilder withBindPort(int bindPort) {
		Preconditions.checkArgument(NetUtils.isValidHostPort(bindPort), "Invalid port number: " + bindPort);
		this.bindPort = bindPort;
		return this;
	}

	public AkkaRpcService createAndStart() throws Exception {
		if (actorSystemExecutorConfiguration == null) {
			actorSystemExecutorConfiguration = BootstrapTools.ForkJoinExecutorConfiguration.fromConfiguration(configuration);
		}

		final ActorSystem actorSystem;

		if (externalAddress == null) {
			// create local actor system
			actorSystem = BootstrapTools.startLocalActorSystem(
				configuration,
				actorSystemName,
				logger,
				actorSystemExecutorConfiguration,
				customConfig);
		} else {
			// create remote actor system
			actorSystem = BootstrapTools.startRemoteActorSystem(
				configuration,
				actorSystemName,
				externalAddress,
				externalPortRange,
				bindAddress,
				bindPort,
				logger,
				actorSystemExecutorConfiguration,
				customConfig);
		}

		return new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.fromConfiguration(configuration));
	}

}
