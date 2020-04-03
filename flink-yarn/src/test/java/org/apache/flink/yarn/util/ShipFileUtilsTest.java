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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.yarn.configuration.YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link ShipFileUtils}.
 */
public class ShipFileUtilsTest {

	///**
	// * Tests to ship files through the {@code YarnClusterDescriptor.addShipFiles}.
	// */
	//@Test
	//public void testExplicitFileShipping() throws Exception {
	//	try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
	//		descriptor.setLocalJarPath(new Path("/path/to/flink.jar"));
    //
	//		File libFile = temporaryFolder.newFile("libFile.jar");
	//		File libFolder = temporaryFolder.newFolder().getAbsoluteFile();
    //
	//		Assert.assertFalse(descriptor.getShipFiles().contains(libFile));
	//		Assert.assertFalse(descriptor.getShipFiles().contains(libFolder));
    //
	//		List<File> shipFiles = new ArrayList<>();
	//		shipFiles.add(libFile);
	//		shipFiles.add(libFolder);
    //
	//		descriptor.addShipFiles(shipFiles);
    //
	//		Assert.assertTrue(descriptor.getShipFiles().contains(libFile));
	//		Assert.assertTrue(descriptor.getShipFiles().contains(libFolder));
    //
	//		// only execute part of the deployment to test for shipped files
	//		Set<File> effectiveShipFiles = new HashSet<>();
	//		descriptor.addLibFoldersToShipFiles(effectiveShipFiles);
    //
	//		Assert.assertEquals(0, effectiveShipFiles.size());
	//		Assert.assertEquals(2, descriptor.getShipFiles().size());
	//		Assert.assertTrue(descriptor.getShipFiles().contains(libFile));
	//		Assert.assertTrue(descriptor.getShipFiles().contains(libFolder));
	//	}
	//}
    //
	//@Test
	//public void testEnvironmentLibShipping() throws Exception {
	//	testEnvironmentDirectoryShipping(ConfigConstants.ENV_FLINK_LIB_DIR, false);
	//}
    //
	//@Test
	//public void testEnvironmentPluginsShipping() throws Exception {
	//	testEnvironmentDirectoryShipping(ConfigConstants.ENV_FLINK_PLUGINS_DIR, true);
	//}
    //
	//private void testEnvironmentDirectoryShipping(String environmentVariable, boolean onlyShip) throws Exception {
	//	try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
	//		File libFolder = temporaryFolder.newFolder().getAbsoluteFile();
	//		File libFile = new File(libFolder, "libFile.jar");
	//		assertTrue(libFile.createNewFile());
    //
	//		Set<File> effectiveShipFiles = new HashSet<>();
    //
	//		final Map<String, String> oldEnv = System.getenv();
	//		try {
	//			Map<String, String> env = new HashMap<>(1);
	//			env.put(environmentVariable, libFolder.getAbsolutePath());
	//			CommonTestUtils.setEnv(env);
	//			// only execute part of the deployment to test for shipped files
	//			if (onlyShip) {
	//				descriptor.addPluginsFoldersToShipFiles(effectiveShipFiles);
	//			} else {
	//				descriptor.addLibFoldersToShipFiles(effectiveShipFiles);
	//			}
	//		} finally {
	//			CommonTestUtils.setEnv(oldEnv);
	//		}
    //
	//		// only add the ship the folder, not the contents
	//		Assert.assertFalse(effectiveShipFiles.contains(libFile));
	//		Assert.assertTrue(effectiveShipFiles.contains(libFolder));
	//		Assert.assertFalse(descriptor.getShipFiles().contains(libFile));
	//		Assert.assertFalse(descriptor.getShipFiles().contains(libFolder));
	//	}
	//}
    //
	//@Test
	//public void testEnvironmentEmptyPluginsShipping() {
	//	try (YarnClusterDescriptor descriptor = createYarnClusterDescriptor()) {
	//		File pluginsFolder = Paths.get(temporaryFolder.getRoot().getAbsolutePath(), "s0m3_p4th_th4t_sh0uld_n0t_3x1sts").toFile();
	//		Set<File> effectiveShipFiles = new HashSet<>();
    //
	//		final Map<String, String> oldEnv = System.getenv();
	//		try {
	//			Map<String, String> env = new HashMap<>(1);
	//			env.put(ConfigConstants.ENV_FLINK_PLUGINS_DIR, pluginsFolder.getAbsolutePath());
	//			CommonTestUtils.setEnv(env);
	//			// only execute part of the deployment to test for shipped files
	//			descriptor.addPluginsFoldersToShipFiles(effectiveShipFiles);
	//		} finally {
	//			CommonTestUtils.setEnv(oldEnv);
	//		}
    //
	//		assertTrue(effectiveShipFiles.isEmpty());
	//	}
	//}
    //
	//@Test
	//public void testDisableSystemClassPathIncludeUserJarAndWithIllegalShipDirectoryName() throws IOException {
	//	final Configuration configuration = new Configuration();
	//	configuration.setString(CLASSPATH_INCLUDE_USER_JAR, YarnConfigOptions.UserJarInclusion.DISABLED.toString());
    //
	//	final YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor(configuration);
	//	try {
	//		yarnClusterDescriptor.addShipFiles(Collections.singletonList(temporaryFolder.newFolder(ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR)));
	//		fail();
	//	} catch (IllegalArgumentException exception) {
	//		assertThat(exception.getMessage(), containsString("This is an illegal ship directory :"));
	//	}
	//}
}
