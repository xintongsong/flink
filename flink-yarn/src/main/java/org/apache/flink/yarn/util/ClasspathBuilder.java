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

package org.apache.flink.yarn.util;

import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.Path;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for generating classpath for Yarn deployments.
 */
public class ClasspathBuilder {

	private final YarnConfigOptions.UserJarInclusion userJarInclusion;

	private final Set<String> systemArchives = new HashSet<>();
	private final Set<String> systemResources = new HashSet<>();
	private final Set<String> userArchives = new HashSet<>();
	private final Set<String> userResources = new HashSet<>();

	public ClasspathBuilder(final YarnConfigOptions.UserJarInclusion userJarInclusion) {
		this.userJarInclusion = Preconditions.checkNotNull(userJarInclusion);
	}

	public void addClasspath(final String remoteRelativePath, ClasspathType type) {
		addClasspath(new Path(Preconditions.checkNotNull(remoteRelativePath)), type);
	}

	public void addClasspath(final Path remoteRelativePath, ClasspathType type) {
		Preconditions.checkNotNull(remoteRelativePath);
		Preconditions.checkNotNull(type);

		switch (type) {
			case SYSTEM:
				addClasspath(remoteRelativePath, systemArchives, systemResources);
				break;
			case USER:
				addClasspath(remoteRelativePath, userArchives, userResources);
				break;
		}
	}

	private void addClasspath(final Path remoteRelativePath, final Set<String> archives, final Set<String> resources) {
		if (remoteRelativePath.getName().endsWith(".jar")) {
			archives.add(remoteRelativePath.toString());
		} else {
			resources.add(remoteRelativePath.getParent().toString());
		}
	}

	public String build() {
		return getClasspathsSorted().collect(Collectors.joining(File.pathSeparator));
	}

	@Nonnull
	private Stream<String> getClasspathsSorted() {
		switch (userJarInclusion) {
			case DISABLED:
				return getSystemClasspathSorted();
			case FIRST:
				return Stream.concat(getUserClasspathSorted(), getSystemClasspathSorted());
			case LAST:
				return Stream.concat(getSystemClasspathSorted(), getUserClasspathSorted());
			case ORDER:
			default:
				return Stream.concat(
					Stream.concat(systemResources.stream(), userResources.stream()).sorted(),
					Stream.concat(systemArchives.stream(), userArchives.stream()).sorted());
		}
	}

	private Stream<String> getUserClasspathSorted() {
		return Stream.concat(userResources.stream().sorted(), userArchives.stream().sorted());
	}

	private Stream<String> getSystemClasspathSorted() {
		return Stream.concat(systemResources.stream().sorted(), systemArchives.stream().sorted());
	}

	/**
	 * Indicates which classpath to add.
	 */
	public enum ClasspathType {
		SYSTEM,
		USER,
		NONE
	}
}
