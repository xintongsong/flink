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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ClasspathBuilder}.
 */
public class ClasspathBuilderTest extends TestLogger {

	private static final Random rand = new Random();

	@Test
	public void testUserJarDisabled() {
		ClasspathBuilder classpathBuilder = new ClasspathBuilder(YarnConfigOptions.UserJarInclusion.DISABLED);
		List<String> expectedClasspaths = addTestClasspaths(classpathBuilder)
			.stream().filter(s -> s.startsWith("sys")).collect(Collectors.toList());
		List<String> actualClasspaths = Arrays.asList(classpathBuilder.build().split(File.pathSeparator));

		assertThat(actualClasspaths, containsInAnyOrder(expectedClasspaths.toArray()));
		actualClasspaths.forEach(entry -> assertThat(entry, startsWith("sys")));
	}

	@Test
	public void testUserJarFirst() {
		ClasspathBuilder classpathBuilder = new ClasspathBuilder(YarnConfigOptions.UserJarInclusion.FIRST);
		List<String> expectedClasspaths = addTestClasspaths(classpathBuilder);
		List<String> actualClasspaths = Arrays.asList(classpathBuilder.build().split(File.pathSeparator));

		assertThat(actualClasspaths, containsInAnyOrder(expectedClasspaths.toArray()));
		actualClasspaths.subList(0, actualClasspaths.size() / 2).forEach(entry -> assertThat(entry, startsWith("usr")));
		actualClasspaths.subList(actualClasspaths.size() / 2, actualClasspaths.size()).forEach(entry -> assertThat(entry, startsWith("sys")));
	}

	@Test
	public void testUserJarLast() {
		ClasspathBuilder classpathBuilder = new ClasspathBuilder(YarnConfigOptions.UserJarInclusion.LAST);
		List<String> expectedClasspaths = addTestClasspaths(classpathBuilder);
		List<String> actualClasspaths = Arrays.asList(classpathBuilder.build().split(File.pathSeparator));

		assertThat(actualClasspaths, containsInAnyOrder(expectedClasspaths.toArray()));
		actualClasspaths.subList(0, actualClasspaths.size() / 2).forEach(entry -> assertThat(entry, startsWith("sys")));
		actualClasspaths.subList(actualClasspaths.size() / 2, actualClasspaths.size()).forEach(entry -> assertThat(entry, startsWith("usr")));
	}

	@Test
	public void testUserJarOrder() {
		ClasspathBuilder classpathBuilder = new ClasspathBuilder(YarnConfigOptions.UserJarInclusion.ORDER);
		List<String> expectedClasspaths = addTestClasspaths(classpathBuilder);
		expectedClasspaths.sort((e1, e2) -> {
			if (e1.endsWith(".jar") && !e2.endsWith(".jar")) {
				return  1;
			} else if (!e1.endsWith(".jar") && e2.endsWith(".jar")) {
				return -1;
			} else {
				return e1.compareTo(e2);
			}
		});

		List<String> actualClasspaths = Arrays.asList(classpathBuilder.build().split(File.pathSeparator));

		assertThat(actualClasspaths, is(expectedClasspaths));
	}

	private static List<String> addTestClasspaths(ClasspathBuilder classpathBuilder) {
		Tuple2<String, String> systemResource1 = randomPathAndClasspath(true, true);
		Tuple2<String, String> systemResource2 = randomPathAndClasspath(true, true);
		Tuple2<String, String> systemArchive1 = randomPathAndClasspath(true, false);
		Tuple2<String, String> systemArchive2 = randomPathAndClasspath(true, false);
		Tuple2<String, String> userResource1 = randomPathAndClasspath(false, true);
		Tuple2<String, String> userResource2 = randomPathAndClasspath(false, true);
		Tuple2<String, String> userArchive1 = randomPathAndClasspath(false, false);
		Tuple2<String, String> userArchive2 = randomPathAndClasspath(false, false);

		classpathBuilder.addClasspath(systemResource1.f0, ClasspathBuilder.ClasspathType.SYSTEM);
		classpathBuilder.addClasspath(systemResource2.f0, ClasspathBuilder.ClasspathType.SYSTEM);
		classpathBuilder.addClasspath(systemArchive1.f0, ClasspathBuilder.ClasspathType.SYSTEM);
		classpathBuilder.addClasspath(systemArchive2.f0, ClasspathBuilder.ClasspathType.SYSTEM);
		classpathBuilder.addClasspath(userResource1.f0, ClasspathBuilder.ClasspathType.USER);
		classpathBuilder.addClasspath(userResource2.f0, ClasspathBuilder.ClasspathType.USER);
		classpathBuilder.addClasspath(userArchive1.f0, ClasspathBuilder.ClasspathType.USER);
		classpathBuilder.addClasspath(userArchive2.f0, ClasspathBuilder.ClasspathType.USER);

		return Arrays.asList(
			systemResource1.f1,
			systemResource2.f1,
			systemArchive1.f1,
			systemArchive2.f1,
			userResource1.f1,
			userResource2.f1,
			userArchive1.f1,
			userArchive2.f1);
	}

	private static Tuple2<String, String> randomPathAndClasspath(boolean isSystem, boolean isResource) {
		String parent = String.format("%s-%d-dir", isSystem ? "sys" : "usr", rand.nextInt());
		String path = String.format("%s/%d-file%s", parent, rand.nextInt(), isResource ? "" : ".jar");
		String classpath = isResource ? parent : path;
		return Tuple2.of(path, classpath);
	}
}
