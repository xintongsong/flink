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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered2.common;

/**
 * Identifier of a subpartition.
 *
 * <p>A subpartition in the Tiered Storage is a subset of a partition (see {@link
 * TieredStoragePartitionId}). It's equivalent to a result sub-partition in Flink.
 *
 * <p>Note it is possible that a subpartition is consumed by multiple consumers. In such cases, each
 * consumer is expected to consume the entire subpartition as a whole, unless there's a failure.
 *
 * <p>Note it is also possible that a consumer consumes multiple subpartitions from the same
 * partition.
 */
public class TieredStorageSubpartitionId implements TieredStorageDataIdentifier {}
