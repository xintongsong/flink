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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    /** External consumer regions of each ConsumedPartitionGroup. */
    private final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
            partitionGroupConsumerRegions = new IdentityHashMap<>();

    private final Map<SchedulingPipelinedRegion, List<ExecutionVertexID>> regionVerticesSorted =
            new IdentityHashMap<>();

    /** The ConsumedPartitionGroups which are produced by multiple regions. */
    private final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups =
            Collections.newSetFromMap(new IdentityHashMap<>());

    private final Set<SchedulingPipelinedRegion> scheduledRegions = new HashSet<>();

    public PipelinedRegionSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);

        init();
    }

    private void init() {

        initCrossRegionConsumedPartitionGroups();

        initPartitionGroupConsumerRegions();

        for (SchedulingExecutionVertex vertex : schedulingTopology.getVertices()) {
            final SchedulingPipelinedRegion region =
                    schedulingTopology.getPipelinedRegionOfVertex(vertex.getId());
            regionVerticesSorted
                    .computeIfAbsent(region, r -> new ArrayList<>())
                    .add(vertex.getId());
        }
    }

    private void initCrossRegionConsumedPartitionGroups() {
        final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
                producerRegionsByConsumedPartitionGroup = new IdentityHashMap<>();

        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                producerRegionsByConsumedPartitionGroup.computeIfAbsent(
                        consumedPartitionGroup, this::getProducerRegionsForConsumedPartitionGroup);
            }
        }

        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                final Set<SchedulingPipelinedRegion> producerRegions =
                        producerRegionsByConsumedPartitionGroup.get(consumedPartitionGroup);
                if (producerRegions.size() > 1 && producerRegions.contains(pipelinedRegion)) {
                    crossRegionConsumedPartitionGroups.add(consumedPartitionGroup);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getProducerRegionsForConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {
        final Set<SchedulingPipelinedRegion> producerRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            producerRegions.add(getProducerRegion(partitionId));
        }
        return producerRegions;
    }

    private SchedulingPipelinedRegion getProducerRegion(IntermediateResultPartitionID partitionId) {
        return schedulingTopology.getPipelinedRegionOfVertex(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    private void initPartitionGroupConsumerRegions() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    region.getAllNonPipelinedConsumedPartitionGroups()) {
                if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                        || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                    partitionGroupConsumerRegions
                            .computeIfAbsent(consumedPartitionGroup, group -> new HashSet<>())
                            .add(region);
                }
            }
        }
    }

    @Override
    public void startScheduling() {
        final Set<SchedulingPipelinedRegion> sourceRegions =
                IterableUtils.toStream(schedulingTopology.getAllPipelinedRegions())
                        .filter(this::isSourceRegion)
                        .collect(Collectors.toSet());
        maybeScheduleRegions(sourceRegions);
    }

    private boolean isSourceRegion(SchedulingPipelinedRegion region) {
        for (ConsumedPartitionGroup consumedPartitionGroup :
                region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                    || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        final Set<SchedulingPipelinedRegion> regionsToRestart =
                verticesToRestart.stream()
                        .map(schedulingTopology::getPipelinedRegionOfVertex)
                        .collect(Collectors.toSet());
        scheduledRegions.removeAll(regionsToRestart);
        maybeScheduleRegions(regionsToRestart);
    }

    @Override
    public void onExecutionStateChange(
            final ExecutionVertexID executionVertexId, final ExecutionState executionState) {
        if (executionState == ExecutionState.FINISHED) {
            maybeScheduleRegions(getSchedulableDownstreamRegionsOfVertex(executionVertexId));
        }
    }

    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {}

    private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {
        final List<SchedulingPipelinedRegion> regionsSorted =
                SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                        schedulingTopology, regions);

        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();
        final Set<SchedulingPipelinedRegion> downstreamSchedulableRegions = new HashSet<>();
        for (SchedulingPipelinedRegion region : regionsSorted) {
            if (maybeScheduleRegion(region, consumableStatusCache)) {
                downstreamSchedulableRegions.addAll(
                        getSchedulableDownstreamRegionsOfRegion(region));
            }
        }

        if (!downstreamSchedulableRegions.isEmpty()) {
            maybeScheduleRegions(downstreamSchedulableRegions);
        }
    }

    private boolean maybeScheduleRegion(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        if (!areRegionInputsAllConsumable(region, consumableStatusCache)) {
            return false;
        }

        checkState(
                areRegionVerticesAllInCreatedState(region),
                "BUG: trying to schedule a region which is not in CREATED state");

        schedulerOperations.allocateSlotsAndDeploy(regionVerticesSorted.get(region));
        scheduledRegions.add(region);
        return true;
    }

    private boolean areRegionInputsAllConsumable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        for (ConsumedPartitionGroup consumedPartitionGroup :
                region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)) {
                if (!isDownstreamOfCrossRegionConsumedPartitionSchedulable(
                        consumedPartitionGroup, region)) {
                    return false;
                }
            } else if (isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                if (!consumableStatusCache.computeIfAbsent(
                        consumedPartitionGroup,
                        this::isDownstreamConsumedPartitionGroupSchedulable)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamConsumedPartitionGroupSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (!scheduledRegions.contains(getProducerRegion(partitionId))) {
                    return false;
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (schedulingTopology.getResultPartition(partitionId).getState()
                        != ResultPartitionState.CONSUMABLE) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamOfCrossRegionConsumedPartitionSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final SchedulingPipelinedRegion pipelinedRegion) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (!scheduledRegions.contains(getProducerRegion(partitionId))) {
                    return false;
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)
                        && schedulingTopology.getResultPartition(partitionId).getState()
                                != ResultPartitionState.CONSUMABLE) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean areRegionVerticesAllInCreatedState(final SchedulingPipelinedRegion region) {
        for (SchedulingExecutionVertex vertex : region.getVertices()) {
            if (vertex.getState() != ExecutionState.CREATED) {
                return false;
            }
        }
        return true;
    }

    private boolean isExternalConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup,
            SchedulingPipelinedRegion pipelinedRegion) {

        return isExternalConsumedPartition(consumedPartitionGroup.getFirst(), pipelinedRegion);
    }

    private boolean isExternalConsumedPartition(
            IntermediateResultPartitionID partitionId, SchedulingPipelinedRegion pipelinedRegion) {
        return !pipelinedRegion.contains(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    private Set<SchedulingPipelinedRegion> getSchedulableDownstreamRegionsOfVertex(
            ExecutionVertexID executionVertexId) {
        return getSchedulableDownstreamRegionsOfGroups(
                IterableUtils.toStream(
                                schedulingTopology
                                        .getVertex(executionVertexId)
                                        .getProducedResults())
                        .filter(this::downstreamOfPartitionMayBeScheduled)
                        .flatMap(partition -> partition.getConsumedPartitionGroups().stream()));
    }

    private Set<SchedulingPipelinedRegion> getSchedulableDownstreamRegionsOfRegion(
            SchedulingPipelinedRegion region) {
        return getSchedulableDownstreamRegionsOfGroups(
                IterableUtils.toStream(region.getAllNonPipelinedConsumedPartitionGroups()));
    }

    private Set<SchedulingPipelinedRegion> getSchedulableDownstreamRegionsOfGroups(
            Stream<ConsumedPartitionGroup> groups) {
        return groups.filter(this::downstreamOfGroupMayBeScheduled)
                .distinct()
                .flatMap(
                        partitionGroup ->
                                partitionGroupConsumerRegions
                                        .getOrDefault(partitionGroup, Collections.emptySet())
                                        .stream())
                .collect(Collectors.toSet());
    }

    private boolean downstreamOfPartitionMayBeScheduled(SchedulingResultPartition partition) {
        return partition.getResultType().canBePipelinedConsumed()
                ? scheduledRegions.contains(getProducerRegion(partition.getId()))
                : partition.getState() == ResultPartitionState.CONSUMABLE;
    }

    private boolean downstreamOfGroupMayBeScheduled(ConsumedPartitionGroup group) {
        if (crossRegionConsumedPartitionGroups.contains(group)) {
            // for cross region consumed partition groups, leave the decision to maybeScheduleRegion
            return true;
        }

        return group.getResultPartitionType().canBePipelinedConsumed()
                ? scheduledRegions.containsAll(getProducerRegionsForConsumedPartitionGroup(group))
                : group.areAllPartitionsFinished();
    }

    @VisibleForTesting
    Set<ConsumedPartitionGroup> getCrossRegionConsumedPartitionGroups() {
        return Collections.unmodifiableSet(crossRegionConsumedPartitionGroups);
    }

    /** The factory for creating {@link PipelinedRegionSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
