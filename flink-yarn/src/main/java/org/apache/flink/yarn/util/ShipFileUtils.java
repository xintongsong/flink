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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginConfig;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utility class for shipping/removing files in Yarn integration.
 */
public class ShipFileUtils implements AutoCloseable {

	/** Number of total retries to fetch the remote resources after uploaded in case of FileNotFoundException. */
	private static final int REMOTE_RESOURCES_FETCH_NUM_RETRY = 3;

	/** Time to wait in milliseconds between each remote resources fetch in case of FileNotFoundException. */
	private static final int REMOTE_RESOURCES_FETCH_WAIT_IN_MILLI = 100;

	private static final Logger LOG = LoggerFactory.getLogger(ShipFileUtils.class);

	private final Configuration flinkConfiguration;
	private final YarnConfiguration yarnConfiguration;
	private final ApplicationId appId;

	private final FileSystem fs;
	private final int fileReplica;
	private final ClasspathBuilder classpathBuilder;
	private final YarnConfigOptions.UserJarInclusion userJarInclusion;

	/** The files need to be shipped and added to classpath. */
	private final Set<File> systemShipFiles;
	/** The files only need to be shipped. */
	private final Set<File> shipOnlyFiles;
	/** The files need to be shipped and added to classpath. */
	private final Set<File> userJarFiles;

	/** Local resource map for Yarn. */
	private final Map<String, LocalResource> localResources;
	/** List of remote paths (after upload). */
	private final List<Path> remotePaths;
	/** Remote paths that need to be added to task executors environment.*/
	private final Map<String, Path> envPaths;

	private ShipFileUtils(
		final Configuration flinkConfiguration,
		final YarnConfiguration yarnConfiguration,
		final ApplicationId appId,
		final FileSystem fs) {
		this.flinkConfiguration = flinkConfiguration;
		this.yarnConfiguration = yarnConfiguration;
		this.appId = appId;

		this.fs = fs;
		this.fileReplica = getFileReplica();

		this.userJarInclusion = flinkConfiguration.
			getEnum(YarnConfigOptions.UserJarInclusion.class, YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR);
		this.classpathBuilder = new ClasspathBuilder(userJarInclusion);

		this.systemShipFiles = new HashSet<>();
		this.shipOnlyFiles = new HashSet<>();
		this.userJarFiles = new HashSet<>();
		this.localResources = new HashMap<>();
		this.remotePaths = new ArrayList<>();
		this.envPaths = new HashMap<>();

		prepareShipFiles();
	}

	public static ShipFileUtils newInstance(
		final Configuration flinkConfiguration,
		final YarnConfiguration yarnConfiguration,
		final ApplicationId appId) throws IOException {

		return new ShipFileUtils(
			flinkConfiguration,
			yarnConfiguration,
			appId,
			getFileSystem(flinkConfiguration, yarnConfiguration));
	}

	@VisibleForTesting
	public static ShipFileUtils newInstance(
		final Configuration flinkConfiguration,
		final YarnConfiguration yarnConfiguration,
		final ApplicationId appId,
		final FileSystem fs) {

		return new ShipFileUtils(
			flinkConfiguration,
			yarnConfiguration,
			appId,
			fs);
	}

	private static FileSystem getFileSystem(
		final Configuration flinkConfiguration,
		final YarnConfiguration yarnConfiguration) throws IOException {

		org.apache.flink.core.fs.FileSystem.initialize(
			flinkConfiguration,
			PluginUtils.createPluginManagerFromRootFolder(flinkConfiguration));

		final FileSystem fs = FileSystem.get(yarnConfiguration);

		// hard coded check for the GoogleHDFS client because its not overriding the getScheme() method.
		if (!fs.getClass().getSimpleName().equals("GoogleHadoopFileSystem") &&
			fs.getScheme().startsWith("file")) {
			LOG.warn("The file system scheme is '" + fs.getScheme() + "'. This indicates that the "
				+ "specified Hadoop configuration path is wrong and the system is using the default Hadoop configuration values."
				+ "The Flink YARN client needs to store its files in a distributed file system");
		}

		return fs;
	}

	public Path getRemoteHomeDir() {
		return fs.getHomeDirectory();
	}

	private int getFileReplica() {
		final int yarnFileReplication = yarnConfiguration.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, DFSConfigKeys.DFS_REPLICATION_DEFAULT);
		final int flinkFileReplication = flinkConfiguration.getInteger(YarnConfigOptions.FILE_REPLICATION);
		return flinkFileReplication > 0 ? flinkFileReplication : yarnFileReplication;
	}

	public String getClasspath() {
		return classpathBuilder.build();
	}

	public Map<String, LocalResource> getLocalResources() {
		return Collections.unmodifiableMap(localResources);
	}

	public List<Path> getRemotePaths() {
		return Collections.unmodifiableList(remotePaths);
	}

	public String getEnvPaths() {
		return envPaths.entrySet().stream()
			.map(entry -> entry.getKey() + "= " + entry.getValue())
			.collect(Collectors.joining(","));
	}

	public void setPermission(final Path path, final FsPermission permisison) throws IOException {
		fs.setPermission(path, permisison);
	}

	@Override
	public void close() throws Exception {
		fs.close();
	}

	// ------------------------------------------------------------------------
	// Preparing ship files.
	// ------------------------------------------------------------------------

	public void addSystemShipFile(final File file) {
		this.systemShipFiles.add(Preconditions.checkNotNull(file));
	}

	public void addUserJarFiles(final Set<File> userJarFiles) {
		this.userJarFiles.addAll(Preconditions.checkNotNull(userJarFiles));
	}

	private void prepareShipFiles() {
		final int numConfiguredShipFiles = addConfiguredShipDirsToShipFiles();
		final boolean isLibDirConfigured = addLibFoldersToShipFiles();

		if (numConfiguredShipFiles <= 0 && !isLibDirConfigured) {
			LOG.warn("Environment variable '{}' not set and ship files have not been provided manually. " +
				"Not shipping any library files.", ENV_FLINK_LIB_DIR);
		}

		addPluginsFoldersToShipFiles();
	}

	/**
	 * Add configured ship dirs to ship files.
	 * @return number of files added.
	 */
	private int addConfiguredShipDirsToShipFiles() {
		final List<File> shipFiles = ConfigUtils.decodeListFromConfig(flinkConfiguration, YarnConfigOptions.SHIP_DIRECTORIES, File::new);
		checkShipFilesExcludeDefaultUsrLibDir(shipFiles);
		systemShipFiles.addAll(shipFiles.stream().map(File::getAbsoluteFile).collect(Collectors.toList()));
		return shipFiles.size();
	}

	private void checkShipFilesExcludeDefaultUsrLibDir(final List<File> shipFiles) {
		if (YarnConfigOptions.UserJarInclusion.DISABLED == userJarInclusion) {
			return;
		}

		checkArgument(
			shipFiles.stream()
				.filter(File::isDirectory)
				.map(File::getName)
				.noneMatch(name -> name.equals(DEFAULT_FLINK_USR_LIB_DIR)),
			"This is an illegal ship directory : %s. When setting the %s to %s the name of ship directory can not be %s.",
			ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR,
			YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR.key(),
			YarnConfigOptions.UserJarInclusion.DISABLED,
			ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);
	}

	/**
	 * Add lib forders to ship files.
	 * @return whether a lib forder is configured and added.
	 */
	private boolean addLibFoldersToShipFiles() {
		// Add lib folder to the ship files if the environment variable is set.
		// This is for convenience when running from the command-line.
		// (for other files users explicitly set the ship files)
		String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);
		if (libDir != null) {
			File directoryFile = new File(libDir);
			if (directoryFile.isDirectory()) {
				systemShipFiles.add(directoryFile);
			} else {
				throw new YarnClusterDescriptor.YarnDeploymentException("The environment variable '" + ENV_FLINK_LIB_DIR +
					"' is set to '" + libDir + "' but the directory doesn't exist.");
			}
			return true;
		}
		return false;
	}

	private void addPluginsFoldersToShipFiles() {
		// Plugin files only need to be shipped and should not be added to classpath.
		final Optional<File> pluginsDir = PluginConfig.getPluginsDir();
		pluginsDir.ifPresent(shipOnlyFiles::add);
	}

	// ------------------------------------------------------------------------
	// Uploading files and resources.
	// ------------------------------------------------------------------------

	public void uploadShipFiles() throws IOException {
		uploadAndRegisterFiles(systemShipFiles, Path.CUR_DIR, ClasspathBuilder.ClasspathType.SYSTEM);
		uploadAndRegisterFiles(shipOnlyFiles, Path.CUR_DIR, ClasspathBuilder.ClasspathType.NONE);
		uploadAndRegisterFiles(
			userJarFiles,
			userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ?
				ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR : Path.CUR_DIR,
			ClasspathBuilder.ClasspathType.USER);
	}

	/**
	 * Recursively uploads (and registers) any (user and system) files in <tt>shipFiles</tt> except
	 * for files matching "<tt>flink-dist*.jar</tt>" which should be uploaded separately.
	 * @param shipFiles files to upload
	 * @param localResourcesDirectory the directory the localResources are uploaded to
	 * @param classpathType whether and where to add the classpath
	 */
	@VisibleForTesting
	public void uploadAndRegisterFiles(
		Collection<File> shipFiles,
		String localResourcesDirectory,
		ClasspathBuilder.ClasspathType classpathType) throws IOException {

		checkArgument(fileReplica >= 1);
		final List<Path> localPaths = new ArrayList<>();
		final List<Path> relativePaths = new ArrayList<>();
		for (File shipFile : shipFiles) {
			if (shipFile.isDirectory()) {
				// add directories to the classpath
				final java.nio.file.Path shipPath = shipFile.toPath();
				final java.nio.file.Path parentPath = shipPath.getParent();
				Files.walkFileTree(shipPath, new SimpleFileVisitor<java.nio.file.Path>() {
					@Override
					public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) {
						localPaths.add(new Path(file.toUri()));
						relativePaths.add(new Path(localResourcesDirectory, parentPath.relativize(file).toString()));
						return FileVisitResult.CONTINUE;
					}
				});
			} else {
				localPaths.add(new Path(shipFile.toURI()));
				relativePaths.add(new Path(localResourcesDirectory, shipFile.getName()));
			}
		}

		for (int i = 0; i < localPaths.size(); i++) {
			final Path localPath = localPaths.get(i);
			final Path relativePath = relativePaths.get(i);
			if (!isDistJar(relativePath.getName())) {
				final String key = relativePath.toString();
				setupSingleLocalResource(
					key,
					localPath,
					relativePath.getParent().toString(),
					true,
					true,
					classpathType);
			}
		}
	}

	/**
	 * Match file name for "<tt>flink-dist*.jar</tt>" pattern.
	 * @param fileName file name to check
	 * @return true if file is a dist jar
	 */
	private static boolean isDistJar(String fileName) {
		return fileName.startsWith("flink-dist") && fileName.endsWith("jar");
	}

	/**
	 * Uploads and registers a single resource and adds it to <tt>localResources</tt>.
	 * @param key the key to add the resource under
	 * @param localSrcPath local path to the file
	 * @param addToRemotePaths whether the resource should be added to remote paths
	 * @param addToEnvPaths whether the resource should be added to env paths
	 * @param classpathType whether and where to add the classpath
	 */
	public Path setupSingleLocalResource(
			String key,
			Path localSrcPath,
			String relativeTargetPath,
			boolean addToRemotePaths,
			boolean addToEnvPaths,
			ClasspathBuilder.ClasspathType classpathType) throws IOException {
		File localFile = new File(localSrcPath.toUri().getPath());
		Tuple2<Path, Long> remoteFileInfo = uploadLocalFileToRemote(localSrcPath, relativeTargetPath);
		Path remotePath = remoteFileInfo.f0;
		LocalResource resource = registerLocalResource(remotePath, localFile.length(), remoteFileInfo.f1);
		localResources.put(key, resource);

		if (addToRemotePaths) {
			remotePaths.add(remotePath);
		}

		if (addToEnvPaths) {
			envPaths.put(key, remotePath);
		}

		classpathBuilder.addClasspath(new Path(relativeTargetPath, remotePath.getName()), classpathType);
		return remotePath;
	}

	/**
	 * Copy a local file to a remote file system.
	 * @param localSrcPath path to the local file
	 * @param relativeTargetPath relative target path of the file (will be prefixed be the full home directory we set up)
	 * @return Path to remote file (usually hdfs)
	 */
	public Tuple2<Path, Long> uploadLocalFileToRemote(Path localSrcPath, String relativeTargetPath) throws IOException {

		File localFile = new File(localSrcPath.toUri().getPath());
		if (localFile.isDirectory()) {
			throw new IllegalArgumentException("File to copy must not be a directory: " +
				localSrcPath);
		}

		// copy resource to HDFS
		String suffix =
			".flink/"
				+ appId
				+ (relativeTargetPath.isEmpty() ? "" : "/" + relativeTargetPath)
				+ "/" + localSrcPath.getName();

		Path dst = new Path(fs.getHomeDirectory(), suffix);

		LOG.debug("Copying from {} to {} with replication number {}", localSrcPath, dst, fileReplica);
		fs.copyFromLocalFile(false, true, localSrcPath, dst);
		fs.setReplication(dst, (short) fileReplica);

		// Note: If we directly used registerLocalResource(FileSystem, Path) here, we would access the remote
		//       file once again which has problems with eventually consistent read-after-write file
		//       systems. Instead, we decide to wait until the remote file be available.

		FileStatus[] fss = null;
		int iter = 1;
		while (iter <= REMOTE_RESOURCES_FETCH_NUM_RETRY + 1) {
			try {
				fss = fs.listStatus(dst);
				break;
			} catch (FileNotFoundException e) {
				LOG.debug("Got FileNotFoundException while fetching uploaded remote resources at retry num {}", iter);
				try {
					LOG.debug("Sleeping for {}ms", REMOTE_RESOURCES_FETCH_WAIT_IN_MILLI);
					TimeUnit.MILLISECONDS.sleep(REMOTE_RESOURCES_FETCH_WAIT_IN_MILLI);
				} catch (InterruptedException ie) {
					LOG.warn("Failed to sleep for {}ms at retry num {} while fetching uploaded remote resources",
						REMOTE_RESOURCES_FETCH_WAIT_IN_MILLI, iter, ie);
				}
				iter++;
			}
		}

		final long dstModificationTime;
		if (fss != null && fss.length >  0) {
			dstModificationTime = fss[0].getModificationTime();
			LOG.debug("Got modification time {} from remote path {}", dstModificationTime, dst);
		} else {
			dstModificationTime = localFile.lastModified();
			LOG.debug("Failed to fetch remote modification time from {}, using local timestamp {}", dst, dstModificationTime);
		}
		return new Tuple2<>(dst, dstModificationTime);
	}

	/**
	 * Creates a YARN resource for the remote object at the given location.
	 * @param remoteRsrcPath remote location of the resource
	 * @param resourceSize size of the resource
	 * @param resourceModificationTime last modification time of the resource
	 * @return YARN resource
	 */
	private static LocalResource registerLocalResource(
		Path remoteRsrcPath,
		long resourceSize,
		long resourceModificationTime) {
		LocalResource localResource = Records.newRecord(LocalResource.class);
		localResource.setResource(ConverterUtils.getYarnUrlFromURI(remoteRsrcPath.toUri()));
		localResource.setSize(resourceSize);
		localResource.setTimestamp(resourceModificationTime);
		localResource.setType(LocalResourceType.FILE);
		localResource.setVisibility(LocalResourceVisibility.APPLICATION);
		return localResource;
	}
}
