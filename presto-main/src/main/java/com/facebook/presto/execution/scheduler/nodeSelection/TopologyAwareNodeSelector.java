/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.NetworkLocation;
import com.facebook.presto.execution.scheduler.NetworkLocationCache;
import com.facebook.presto.execution.scheduler.NodeAssignmentStats;
import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.execution.scheduler.ResettableRandomizedIterator;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static com.facebook.presto.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static com.facebook.presto.execution.scheduler.NodeScheduler.randomizedNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectExactNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * 拓扑感知节点选择器
 *  这个节点选择器，会选择一个最优的节点。bestNodeSplitCount
 * TODO 什么事网络拓扑呢？
 * 在这里可以理解为，多个Worker节点间的连接，彼此知道各自的位置，
 */
public class TopologyAwareNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(TopologyAwareNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeSelectionStats nodeSelectionStats;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final int maxUnacknowledgedSplitsPerTask;
    private final List<CounterStat> topologicalSplitCounters;
    private final List<String> networkLocationSegmentNames;
    private final NetworkLocationCache networkLocationCache;

    public TopologyAwareNodeSelector(
            InternalNodeManager nodeManager,
            NodeSelectionStats nodeSelectionStats,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
            int maxUnacknowledgedSplitsPerTask,
            List<CounterStat> topologicalSplitCounters,
            List<String> networkLocationSegmentNames,
            NetworkLocationCache networkLocationCache)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeSelectionStats = requireNonNull(nodeSelectionStats, "nodeSelectionStats is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        this.maxUnacknowledgedSplitsPerTask = maxUnacknowledgedSplitsPerTask;
        checkArgument(maxUnacknowledgedSplitsPerTask > 0, "maxUnacknowledgedSplitsPerTask must be > 0, found: %s", maxUnacknowledgedSplitsPerTask);
        this.topologicalSplitCounters = requireNonNull(topologicalSplitCounters, "topologicalSplitCounters is null");
        this.networkLocationSegmentNames = requireNonNull(networkLocationSegmentNames, "networkLocationSegmentNames is null");
        this.networkLocationCache = requireNonNull(networkLocationCache, "networkLocationCache is null");
    }

    @Override
    public void lockDownNodes()
    {
        nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
    }

    @Override
    public List<InternalNode> getActiveNodes()
    {
        return ImmutableList.copyOf(nodeMap.get().get().getActiveNodes());
    }

    @Override
    public List<InternalNode> getAllNodes()
    {
        return ImmutableList.copyOf(nodeMap.get().get().getAllNodes());
    }

    @Override
    public InternalNode selectCurrentNode()
    {
        // TODO: this is a hack to force scheduling on the coordinator
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        return selectNodes(limit, randomizedNodes(nodeMap.get().get(), includeCoordinator, excludedNodes));
    }

    /**
     * TODO 将splits分配到对应的节点, 这个方法很重要
     * @param splits the splits that need to be assigned to nodes
     * @param existingTasks
     * @return
     *
     * 将所有活跃的工作节点作为候选节点；
     *
     * 如果分片的节点选择策略是HARD_AFFINITY，即分片只能在特定节点进行访问，则根据分片要求更新候选节点列表；
     *
     * 如果分片的节点选择策略不是HARD_AFFINITY，则根据节点的网络拓扑，从候选节点中选择和分片偏好节点网络路径最匹配的节点列表来更新候选节点列表；
     *
     * 使用bestNodeSplitCount方法从更新后的候选节点列表中选择最合适的节点来分配分片；
     */
    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        // 网络节点
        NodeMap nodeMap = this.nodeMap.get().get();
        // 每个节点分配的split，使用Multimap进行存储，因为一个节点可能会分配多个split
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        // 节点分配统计对象，用来存储节点分配信息
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        int[] topologicCounters = new int[topologicalSplitCounters.size()];
        // 已填充的位置
        Set<NetworkLocation> filledLocations = new HashSet<>();
        // 已阻塞的节点
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;

        // 获取活跃的节点列表
        List<HostAddress> candidates = nodeMap.getActiveNodes().stream()
                .map(InternalNode::getHostAndPort)
                .collect(toImmutableList());

        // 遍历所有split
        for (Split split : splits) {
            // 如果分片的节点选择策略是HARD_AFFINITY，即分片只能在特定节点进行访问，则根据分片要求更新候选节点列表；
            if (split.getNodeSelectionStrategy() == HARD_AFFINITY) {
                // 候选节点
                List<InternalNode> candidateNodes = selectExactNodes(nodeMap, split.getPreferredNodes(candidates), includeCoordinator);
                if (candidateNodes.isEmpty()) {
                    log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getActiveNodes());
                    throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                }
                // 选择最合适的节点
                InternalNode chosenNode = bestNodeSplitCount(candidateNodes.iterator(), minCandidates, maxPendingSplitsPerTask, assignmentStats);
                if (chosenNode != null) {
                    assignment.put(chosenNode, split);
                    assignmentStats.addAssignedSplit(chosenNode);
                }
                // Exact node set won't matter, if a split is waiting for any node
                // 如果没有找到合适的节点，且有分片在等待某个节点，则将候选节点全部加入到阻塞节点列表中
                else if (!splitWaitingForAnyNode) {
                    blockedExactNodes.addAll(candidateNodes);
                }
                continue;
            }

            InternalNode chosenNode = null;
            int depth = networkLocationSegmentNames.size();
            int chosenDepth = 0;
            // 网络拓扑中的一个位置，
            Set<NetworkLocation> locations = new HashSet<>();
            for (HostAddress host : split.getPreferredNodes(candidates)) {
                locations.add(networkLocationCache.get(host));
            }
            if (locations.isEmpty()) {
                // Add the root location
                locations.add(ROOT_LOCATION);
                depth = 0;
            }
            // Try each address at progressively shallower network locations
            // 在逐渐变浅的网络位置尝试每个地址，也就是说更倾向于分配层次更浅的符合要求的节点
            for (int i = depth; i >= 0 && chosenNode == null; i--) {
                for (NetworkLocation location : locations) {
                    // Skip locations which are only shallower than this level
                    // For example, locations which couldn't be located will be at the "root" location
                    if (location.getSegments().size() < i) {
                        continue;
                    }
                    location = location.subLocation(0, i);
                    if (filledLocations.contains(location)) {
                        continue;
                    }
                    // 通过网络路径获取活跃的Worker
                    Set<InternalNode> nodes = nodeMap.getActiveWorkersByNetworkPath().get(location);
                    // 将活跃节点随机打散，再次获取最合适的节点
                    chosenNode = bestNodeSplitCount(new ResettableRandomizedIterator<>(nodes), minCandidates, calculateMaxPendingSplits(i, depth), assignmentStats);
                    // 如果找到合适的节点，则跳出
                    if (chosenNode != null) {
                        chosenDepth = i;
                        break;
                    }
                    // 将选中的节点加入已填充位置
                    filledLocations.add(location);
                }
            }
            if (chosenNode != null) {
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode);
                topologicCounters[chosenDepth]++;
            }
            else {
                splitWaitingForAnyNode = true;
            }
        }
        for (int i = 0; i < topologicCounters.length; i++) {
            if (topologicCounters[i] > 0) {
                topologicalSplitCounters.get(i).update(topologicCounters[i]);
            }
        }

        ListenableFuture<?> blocked;
        int maxPendingForWildcardNetworkAffinity = calculateMaxPendingSplits(0, networkLocationSegmentNames.size());
        if (splitWaitingForAnyNode) {
            blocked = toWhenHasSplitQueueSpaceFuture(existingTasks, calculateLowWatermark(maxPendingForWildcardNetworkAffinity));
        }
        else {
            blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks, calculateLowWatermark(maxPendingForWildcardNetworkAffinity));
        }
        return new SplitPlacementResult(blocked, assignment);
    }

    /**
     * Computes how much of the queue can be filled by splits with the network topology distance to a node given by
     * splitAffinity. A split with zero affinity can only fill half the queue, whereas one that matches
     * exactly can fill the entire queue.
     */
    private int calculateMaxPendingSplits(int splitAffinity, int totalDepth)
    {
        if (totalDepth == 0) {
            return maxPendingSplitsPerTask;
        }
        // Use half the queue for any split
        // Reserve the other half for splits that have some amount of network affinity
        double queueFraction = 0.5 * (1.0 + splitAffinity / (double) totalDepth);
        return (int) Math.ceil(maxPendingSplitsPerTask * queueFraction);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsPerNode, maxPendingSplitsPerTask, maxUnacknowledgedSplitsPerTask, splits, existingTasks, bucketNodeMap, nodeSelectionStats);
    }

    /**
     * 最优节点算法
     * 通过节点上的split数量，判断那个是最合适的节点
     * @param candidates
     * @param minCandidatesWhenFull
     * @param maxPendingSplitsPerTask
     * @param assignmentStats
     * @return
     *
     * 从候选节点列表中随机选择一个（传入节点列表时已进行随机打散new ResettableRandomizedIterator<>(nodes)），如果该节点已分配的Split尚未达到阈值，则选择该节点；
     *
     * 如果第1步选择的节点已分配Split达到上限，则选择剩余节点中在当前stage中排队Split最少的节点。
     */
    @Nullable
    private InternalNode bestNodeSplitCount(Iterator<InternalNode> candidates, int minCandidatesWhenFull, int maxPendingSplitsPerTask, NodeAssignmentStats assignmentStats)
    {
        InternalNode bestQueueNotFull = null;
        int min = Integer.MAX_VALUE;
        int fullCandidatesConsidered = 0;

        while (candidates.hasNext() && (fullCandidatesConsidered < minCandidatesWhenFull || bestQueueNotFull == null)) {
            InternalNode node = candidates.next();
            // 是否有可分配split的空间
            boolean hasUnacknowledgedSplitSpace = assignmentStats.getUnacknowledgedSplitCountForStage(node) < maxUnacknowledgedSplitsPerTask;
            // 如果可分配且没有超过最大限制，则选中该节点
            if (hasUnacknowledgedSplitSpace && assignmentStats.getTotalSplitCount(node) < maxSplitsPerNode) {
                return node;
            }
            fullCandidatesConsidered++;
            int totalSplitCount = assignmentStats.getQueuedSplitCountForStage(node);
            if (hasUnacknowledgedSplitSpace && totalSplitCount < min && totalSplitCount < maxPendingSplitsPerTask) {
                min = totalSplitCount;
                bestQueueNotFull = node;
            }
        }
        return bestQueueNotFull;
    }
}
