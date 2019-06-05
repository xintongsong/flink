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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NetworkEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.flink.util.MathUtils.checkedDownCast;

/**
 * Configuration object for the network stack.
 */
public class NetworkEnvironmentConfiguration {
	private static final Logger LOG = LoggerFactory.getLogger(NetworkEnvironmentConfiguration.class);

	private final int numNetworkBuffers;

	private final int networkBufferSize;

	private final int partitionRequestInitialBackoff;

	private final int partitionRequestMaxBackoff;

	/** Number of network buffers to use for each outgoing/incoming channel (subpartition/input channel). */
	private final int networkBuffersPerChannel;

	/** Number of extra network buffers to use for each outgoing/incoming gate (result partition/input gate). */
	private final int floatingNetworkBuffersPerGate;

	private final boolean isCreditBased;

	private final boolean isNetworkDetailedMetrics;

	private final NettyConfig nettyConfig;

	public NetworkEnvironmentConfiguration(
			int numNetworkBuffers,
			int networkBufferSize,
			int partitionRequestInitialBackoff,
			int partitionRequestMaxBackoff,
			int networkBuffersPerChannel,
			int floatingNetworkBuffersPerGate,
			boolean isCreditBased,
			boolean isNetworkDetailedMetrics,
			@Nullable NettyConfig nettyConfig) {

		this.numNetworkBuffers = numNetworkBuffers;
		this.networkBufferSize = networkBufferSize;
		this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
		this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		this.isCreditBased = isCreditBased;
		this.isNetworkDetailedMetrics = isNetworkDetailedMetrics;
		this.nettyConfig = nettyConfig;
	}

	// ------------------------------------------------------------------------

	public int numNetworkBuffers() {
		return numNetworkBuffers;
	}

	public int networkBufferSize() {
		return networkBufferSize;
	}

	public int partitionRequestInitialBackoff() {
		return partitionRequestInitialBackoff;
	}

	public int partitionRequestMaxBackoff() {
		return partitionRequestMaxBackoff;
	}

	public int networkBuffersPerChannel() {
		return networkBuffersPerChannel;
	}

	public int floatingNetworkBuffersPerGate() {
		return floatingNetworkBuffersPerGate;
	}

	public NettyConfig nettyConfig() {
		return nettyConfig;
	}

	public boolean isCreditBased() {
		return isCreditBased;
	}

	public boolean isNetworkDetailedMetrics() {
		return isNetworkDetailedMetrics;
	}

	// ------------------------------------------------------------------------

	/**
	 * Utility method to extract network related parameters from the configuration and to
	 * sanity check them.
	 *
	 * @param configuration configuration object
	 * @param localTaskManagerCommunication true, to skip initializing the network stack
	 * @param taskManagerAddress identifying the IP address under which the TaskManager will be accessible
	 * @return NetworkEnvironmentConfiguration
	 */
	public static NetworkEnvironmentConfiguration fromConfiguration(
		Configuration configuration,
		boolean localTaskManagerCommunication,
		InetAddress taskManagerAddress) {

		final int dataport = getDataport(configuration);

		final int pageSize = getPageSize(configuration);

		final int numberOfNetworkBuffers = calculateNumberOfNetworkBuffers(configuration);

		final NettyConfig nettyConfig = createNettyConfig(configuration, localTaskManagerCommunication, taskManagerAddress, dataport);

		int initialRequestBackoff = configuration.getInteger(NetworkEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
		int maxRequestBackoff = configuration.getInteger(NetworkEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX);

		int buffersPerChannel = configuration.getInteger(NetworkEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
		int extraBuffersPerGate = configuration.getInteger(NetworkEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);

		boolean isCreditBased = nettyConfig != null && configuration.getBoolean(NetworkEnvironmentOptions.NETWORK_CREDIT_MODEL);

		boolean isNetworkDetailedMetrics = configuration.getBoolean(NetworkEnvironmentOptions.NETWORK_DETAILED_METRICS);

		return new NetworkEnvironmentConfiguration(
			numberOfNetworkBuffers,
			pageSize,
			initialRequestBackoff,
			maxRequestBackoff,
			buffersPerChannel,
			extraBuffersPerGate,
			isCreditBased,
			isNetworkDetailedMetrics,
			nettyConfig);
	}

	/**
	 * Parses the hosts / ports for communication and data exchange from configuration.
	 *
	 * @param configuration configuration object
	 * @return the data port
	 */
	private static int getDataport(Configuration configuration) {
		final int dataport = configuration.getInteger(NetworkEnvironmentOptions.DATA_PORT);
		ConfigurationParserUtils.checkConfigParameter(dataport >= 0, dataport, NetworkEnvironmentOptions.DATA_PORT.key(),
			"Leave config parameter empty or use 0 to let the system choose a port automatically.");

		return dataport;
	}

	/**
	 * Calculates the number of network buffers based on configuration.
	 *
	 * @param configuration configuration object
	 * @return the number of network buffers
	 */
	private static int calculateNumberOfNetworkBuffers(Configuration configuration) {
		final long networkMemorySizeByte = MemorySize.parseBytes(
			configuration.getString(TaskManagerOptions.TASK_MANAGER_MEMORY_NETWORK_SIZE_KEY, ""));

		// tolerate offcuts between intended and allocated memory due to segmentation (will be available to the user-space memory)
		long numberOfNetworkBuffersLong = networkMemorySizeByte / getPageSize(configuration);
		if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("The given number of memory bytes (" + networkMemorySizeByte
				+ ") corresponds to more than MAX_INT pages.");
		}

		return (int) numberOfNetworkBuffersLong;
	}

	/**
	 * Generates {@link NettyConfig} from Flink {@link Configuration}.
	 *
	 * @param configuration configuration object
	 * @param localTaskManagerCommunication true, to skip initializing the network stack
	 * @param taskManagerAddress identifying the IP address under which the TaskManager will be accessible
	 * @param dataport data port for communication and data exchange
	 * @return the netty configuration or {@code null} if communication is in the same task manager
	 */
	@Nullable
	private static NettyConfig createNettyConfig(
		Configuration configuration,
		boolean localTaskManagerCommunication,
		InetAddress taskManagerAddress,
		int dataport) {

		final NettyConfig nettyConfig;
		if (!localTaskManagerCommunication) {
			final InetSocketAddress taskManagerInetSocketAddress = new InetSocketAddress(taskManagerAddress, dataport);

			nettyConfig = new NettyConfig(taskManagerInetSocketAddress.getAddress(), taskManagerInetSocketAddress.getPort(),
				getPageSize(configuration), ConfigurationParserUtils.getSlot(configuration), configuration);
		} else {
			nettyConfig = null;
		}

		return nettyConfig;
	}

	/**
	 * Parses the configuration to get the page size and validates the value.
	 *
	 * @param configuration configuration object
	 * @return size of memory segment
	 */
	public static int getPageSize(Configuration configuration) {
		final int pageSize = checkedDownCast(MemorySize.parse(
			configuration.getString(TaskManagerOptions.MEMORY_SEGMENT_SIZE)).getBytes());

		// check page size of for minimum size
		ConfigurationParserUtils.checkConfigParameter(pageSize >= MemoryManager.MIN_PAGE_SIZE, pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE);
		// check page size for power of two
		ConfigurationParserUtils.checkConfigParameter(MathUtils.isPowerOf2(pageSize), pageSize,
			TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
			"Memory segment size must be a power of 2.");

		return pageSize;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + numNetworkBuffers;
		result = 31 * result + networkBufferSize;
		result = 31 * result + partitionRequestInitialBackoff;
		result = 31 * result + partitionRequestMaxBackoff;
		result = 31 * result + networkBuffersPerChannel;
		result = 31 * result + floatingNetworkBuffersPerGate;
		result = 31 * result + (isCreditBased ? 1 : 0);
		result = 31 * result + (nettyConfig != null ? nettyConfig.hashCode() : 0);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		else {
			final NetworkEnvironmentConfiguration that = (NetworkEnvironmentConfiguration) obj;

			return this.numNetworkBuffers == that.numNetworkBuffers &&
					this.networkBufferSize == that.networkBufferSize &&
					this.partitionRequestInitialBackoff == that.partitionRequestInitialBackoff &&
					this.partitionRequestMaxBackoff == that.partitionRequestMaxBackoff &&
					this.networkBuffersPerChannel == that.networkBuffersPerChannel &&
					this.floatingNetworkBuffersPerGate == that.floatingNetworkBuffersPerGate &&
					this.isCreditBased == that.isCreditBased &&
					(nettyConfig != null ? nettyConfig.equals(that.nettyConfig) : that.nettyConfig == null);
		}
	}

	@Override
	public String toString() {
		return "NetworkEnvironmentConfiguration{" +
				", numNetworkBuffers=" + numNetworkBuffers +
				", networkBufferSize=" + networkBufferSize +
				", partitionRequestInitialBackoff=" + partitionRequestInitialBackoff +
				", partitionRequestMaxBackoff=" + partitionRequestMaxBackoff +
				", networkBuffersPerChannel=" + networkBuffersPerChannel +
				", floatingNetworkBuffersPerGate=" + floatingNetworkBuffersPerGate +
				", isCreditBased=" + isCreditBased +
				", nettyConfig=" + nettyConfig +
				'}';
	}
}
