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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractResourceProvider.
 */
public abstract class AbstractResourceProvider<WorkerType extends ResourceIDRetrievable>
	implements ResourceProvider<WorkerType> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected final Configuration flinkConfig;
	protected final Configuration flinkClientConfig;

	private ResourceEventListener<WorkerType> resourceEventListener = null;

	public AbstractResourceProvider(
			final Configuration flinkConfig,
			final Configuration flinkClientConfg) {
		this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
		this.flinkClientConfig = Preconditions.checkNotNull(flinkClientConfg);
	}

	@Override
	public final void initialize(ResourceEventListener<WorkerType> resourceEventListener) throws Throwable {
		this.resourceEventListener = Preconditions.checkNotNull(resourceEventListener);
		initializeInternal();
	}

	protected final ResourceEventListener<WorkerType> getResourceEventListener() {
		Preconditions.checkState(this.resourceEventListener != null, "Not initialized.");
		return this.resourceEventListener;
	}

	protected abstract void initializeInternal() throws Throwable;
}
