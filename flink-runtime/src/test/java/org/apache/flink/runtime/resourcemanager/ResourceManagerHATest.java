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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** ResourceManager HA test, including grant leadership and revoke leadership. */
public class ResourceManagerHATest extends TestLogger {

    @Test
    public void testGrantAndRevokeLeadership() throws Exception {
        final CompletableFuture<UUID> confirmLeadershipFuture = new CompletableFuture<>();

        final TestingResourceManagerService resourceManagerService =
                TestingResourceManagerService.newBuilder()
                        .setRmLeaderElectionService(
                                new TestingLeaderElectionService() {
                                    @Override
                                    public void confirmLeadership(
                                            UUID leaderId, String leaderAddress) {
                                        confirmLeadershipFuture.complete(leaderId);
                                    }
                                })
                        .build();

        try {
            resourceManagerService.start();

            final UUID leaderId = UUID.randomUUID();
            resourceManagerService.isLeader(leaderId);

            // after grant leadership, verify resource manager is started with the fencing token
            assertEquals(leaderId, confirmLeadershipFuture.get());
            assertTrue(resourceManagerService.getResourceManagerFencingToken().isPresent());
            assertEquals(
                    leaderId,
                    resourceManagerService.getResourceManagerFencingToken().get().toUUID());

            // then revoke leadership, verify resource manager is closed
            resourceManagerService.notLeader();
            resourceManagerService.getTerminationFuture().get();

            resourceManagerService.rethrowFatalErrorIfAny();
        } finally {
            resourceManagerService.cleanUp();
        }
    }
}
