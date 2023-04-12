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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/** Utils for reading or writing to tiered store. */
public class TieredStoreUtils {

    private static final String TIER_STORE_DIR = "tiered-store";

    public static Path generateToReleasePath(JobID jobID, String baseDfsPath) {
        if (jobID == null || baseDfsPath == null) {
            return null;
        }

        while (baseDfsPath.endsWith("/") && baseDfsPath.length() > 1) {
            baseDfsPath = baseDfsPath.substring(0, baseDfsPath.length() - 1);
        }
        String basePathStr = String.format("%s/%s/%s", baseDfsPath, TIER_STORE_DIR, jobID);
        return new Path(basePathStr);
    }

    public static void deletePath(Path path) throws IOException {
        if (path == null) {
            return;
        }
        FileSystem fs = path.getFileSystem();
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }
}
