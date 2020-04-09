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

package org.apache.flink.yarn.configuration;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerImpl;
import org.apache.flink.yarn.YarnResourceManager;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Yarn configuration options that are not meant to be set by the user.
 */
@Internal
public class YarnConfigOptionsInternal {

	public static final ConfigOption<String> APPLICATION_LOG_CONFIG_FILE =
			key("$internal.yarn.log-config-file")
					.stringType()
					.noDefaultValue()
					.withDescription("**DO NOT USE** The location of the log config file, e.g. the path to your log4j.properties for log4j.");

	/**
	 * **DO NO USE** Whether {@link YarnResourceManager} should match the vcores of allocated containers with those requested.
	 *
	 * <p>By default, Yarn ignores vcores in the container requests, and always allocate 1 vcore for each container.
	 * Iff 'yarn.scheduler.capacity.resource-calculator' is set to 'DominantResourceCalculator' for Yarn, will it
	 * allocate container vcores as requested. Unfortunately, this configuration option is dedicated for Yarn Scheduler,
	 * and is only accessible to applications in Hadoop 2.6+.
	 *
	 * <p>ATM, it should be fine to not match vcores, because with the current {@link SlotManagerImpl} all the TM
	 * containers should have the same resources.
	 *
	 * <p>If later we add another {@link SlotManager} implementation that may have TMs with different resources, we can
	 * switch this option on only for the new SM, and the new SM can also be available on Hadoop 2.6+ only.
	 */
	public static final ConfigOption<Boolean> MATCH_CONTAINER_VCORES =
			key("$internal.yarn.resourcemanager.enable-vcore-matching")
					.booleanType()
					.defaultValue(false)
					.withDescription("**DO NOT USE** Whether YarnResourceManager should match the container vcores.");
}
