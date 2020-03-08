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

package org.apache.flink.yarn;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A Yarn {@link AMRMClientAsync} implementation for testing.
 */
public class TestingYarnAMRMClientAsync extends AMRMClientAsyncImpl<AMRMClient.ContainerRequest> {

	private Function<Tuple4<Priority, String, Resource, CallbackHandler>, List<? extends Collection<AMRMClient.ContainerRequest>>>
		getMatchingRequestsFunction = ignored -> Collections.emptyList();
	private Consumer<Tuple2<AMRMClient.ContainerRequest, CallbackHandler>> addContainerRequestConsumer = ignored -> {};
	private Consumer<Tuple2<AMRMClient.ContainerRequest, CallbackHandler>> removeContainerRequestConsumer = ignored -> {};
	private Consumer<Tuple2<ContainerId, CallbackHandler>> releaseAssignedContainerConsumer = ignored -> {};

	TestingYarnAMRMClientAsync(CallbackHandler callbackHandler) {
		super(0, callbackHandler);
	}

	@Override
	public List<? extends Collection<AMRMClient.ContainerRequest>> getMatchingRequests(Priority priority, String resourceName, Resource capability) {
		return getMatchingRequestsFunction.apply(Tuple4.of(priority, resourceName, capability, handler));
	}

	@Override
	public void addContainerRequest(AMRMClient.ContainerRequest req) {
		addContainerRequestConsumer.accept(Tuple2.of(req, handler));
	}

	@Override
	public void removeContainerRequest(AMRMClient.ContainerRequest req) {
		removeContainerRequestConsumer.accept(Tuple2.of(req, handler));
	}

	@Override
	public void releaseAssignedContainer(ContainerId containerId) {
		releaseAssignedContainerConsumer.accept(Tuple2.of(containerId, handler));
	}

	void setGetMatchingRequestsFunction(
		Function<Tuple4<Priority, String, Resource, CallbackHandler>, List<? extends Collection<AMRMClient.ContainerRequest>>>
			getMatchingRequestsFunction) {
		this.getMatchingRequestsFunction = Preconditions.checkNotNull(getMatchingRequestsFunction);
	}

	void setAddContainerRequestConsumer(
		Consumer<Tuple2<AMRMClient.ContainerRequest, CallbackHandler>> addContainerRequestConsumer) {
		this.addContainerRequestConsumer = Preconditions.checkNotNull(addContainerRequestConsumer);
	}

	void setRemoveContainerRequestConsumer(
		Consumer<Tuple2<AMRMClient.ContainerRequest, CallbackHandler>> removeContainerRequestConsumer) {
		this.removeContainerRequestConsumer = Preconditions.checkNotNull(removeContainerRequestConsumer);
	}

	void setReleaseAssignedContainerConsumer(
		Consumer<Tuple2<ContainerId, CallbackHandler>> releaseAssignedContainerConsumer) {
		this.releaseAssignedContainerConsumer = Preconditions.checkNotNull(releaseAssignedContainerConsumer);
	}

	// ------------------------------------------------------------------------
	//  Override lifecycle methods to avoid actually starting the service
	// ------------------------------------------------------------------------

	@Override
	protected void serviceInit(Configuration conf) throws Exception {
		// noop
	}

	@Override
	protected void serviceStart() throws Exception {
		// noop
	}

	@Override
	protected void serviceStop() throws Exception {
		// noop
	}
}
