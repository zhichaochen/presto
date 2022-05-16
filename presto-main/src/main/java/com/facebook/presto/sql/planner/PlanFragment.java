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
package com.facebook.presto.sql.planner;

import com.facebook.airlift.json.Codec;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * 每个ExchangeNode对应一个SubPlan对应一个PlanFragment
 * PlanFragment 与 Stage有什么关系呢？
 * 我认为：Stage表示一系列的PlanFragment，Stage表示多个阶段、Fragment表示一个阶段的多个计划片段。
 */
public class PlanFragment
{
    private final PlanFragmentId id; // 段ID
    private final PlanNode root; // 根计划节点
    private final Set<VariableReferenceExpression> variables; // 变量列表
    private final PartitioningHandle partitioning; // 端分割句柄
    private final List<PlanNodeId> tableScanSchedulingOrder; // 表扫描顺序
    private final List<Type> types; // 字段类型
    private final List<RemoteSourceNode> remoteSourceNodes; // Remote数据源节点列表（不同阶段数据交互的节点）
    private final PartitioningScheme partitioningScheme; // 表分割句柄
    private final StageExecutionDescriptor stageExecutionDescriptor; // 阶段执行描述
    private final boolean outputTableWriterFragment; // 是否是输出
    private final StatsAndCosts statsAndCosts; // 统计和耗时
    private final Optional<String> jsonRepresentation; // 表示阶段的json字符串，比如这个节点都有哪些节点等。

    // This is ensured to be lazily populated on the first successful call to #toBytes
    @GuardedBy("this")
    private byte[] cachedSerialization;
    @GuardedBy("this")
    private Codec<PlanFragment> lastUsedCodec;

    @JsonCreator
    public PlanFragment(
            @JsonProperty("id") PlanFragmentId id,
            @JsonProperty("root") PlanNode root,
            @JsonProperty("variables") Set<VariableReferenceExpression> variables,
            @JsonProperty("partitioning") PartitioningHandle partitioning,
            @JsonProperty("tableScanSchedulingOrder") List<PlanNodeId> tableScanSchedulingOrder,
            @JsonProperty("partitioningScheme") PartitioningScheme partitioningScheme,
            @JsonProperty("stageExecutionDescriptor") StageExecutionDescriptor stageExecutionDescriptor,
            @JsonProperty("outputTableWriterFragment") boolean outputTableWriterFragment,
            @JsonProperty("statsAndCosts") StatsAndCosts statsAndCosts,
            @JsonProperty("jsonRepresentation") Optional<String> jsonRepresentation)
    {
        this.id = requireNonNull(id, "id is null");
        this.root = requireNonNull(root, "root is null");
        this.variables = requireNonNull(variables, "variables is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.tableScanSchedulingOrder = ImmutableList.copyOf(requireNonNull(tableScanSchedulingOrder, "tableScanSchedulingOrder is null"));
        this.stageExecutionDescriptor = requireNonNull(stageExecutionDescriptor, "stageExecutionDescriptor is null");
        this.outputTableWriterFragment = outputTableWriterFragment;
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
        this.jsonRepresentation = requireNonNull(jsonRepresentation, "jsonRepresentation is null");

        checkArgument(root.getOutputVariables().containsAll(partitioningScheme.getOutputLayout()),
                "Root node outputs (%s) does not include all fragment outputs (%s)", root.getOutputVariables(), partitioningScheme.getOutputLayout());

        // 字段类型列表
        types = partitioningScheme.getOutputLayout().stream()
                .map(VariableReferenceExpression::getType)
                .collect(toImmutableList());

        // 递归查找所有Remote节点，并加入remoteSourceNodes集合
        ImmutableList.Builder<RemoteSourceNode> remoteSourceNodes = ImmutableList.builder();
        findRemoteSourceNodes(root, remoteSourceNodes);
        this.remoteSourceNodes = remoteSourceNodes.build();

        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
    }

    @JsonProperty
    public PlanFragmentId getId()
    {
        return id;
    }

    @JsonProperty
    public PlanNode getRoot()
    {
        return root;
    }

    @JsonProperty
    public Set<VariableReferenceExpression> getVariables()
    {
        return variables;
    }

    @JsonProperty
    public PartitioningHandle getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public List<PlanNodeId> getTableScanSchedulingOrder()
    {
        return tableScanSchedulingOrder;
    }

    @JsonProperty
    public PartitioningScheme getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @JsonProperty
    public StageExecutionDescriptor getStageExecutionDescriptor()
    {
        return stageExecutionDescriptor;
    }

    @JsonProperty
    public boolean isOutputTableWriterFragment()
    {
        return outputTableWriterFragment;
    }

    @JsonProperty
    public StatsAndCosts getStatsAndCosts()
    {
        return statsAndCosts;
    }

    @JsonProperty
    public Optional<String> getJsonRepresentation()
    {
        // @reviewer: I believe this should be a json raw value, but that would make this class have a different deserialization constructor.
        // workers don't need this, so that should be OK, but it's worth thinking about.
        return jsonRepresentation;
    }

    // Serialize this plan fragment with the provided codec, caching the results
    public synchronized byte[] toBytes(Codec<PlanFragment> codec)
    {
        requireNonNull(codec, "codec is null");
        if (cachedSerialization != null) {
            verify(codec == lastUsedCodec, "Only one Codec may be used to serialize PlanFragments");
        }
        else {
            cachedSerialization = codec.toBytes(this);
            lastUsedCodec = codec;
        }
        return cachedSerialization;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    /**
     * 是否是叶子节点
     * @return
     */
    public boolean isLeaf()
    {
        return remoteSourceNodes.isEmpty();
    }

    public List<RemoteSourceNode> getRemoteSourceNodes()
    {
        return remoteSourceNodes;
    }

    /**
     * 查找节点的子节点
     * @param node
     * @param nodeIds
     * @return
     */
    private static Set<PlanNode> findSources(PlanNode node, Iterable<PlanNodeId> nodeIds)
    {
        ImmutableSet.Builder<PlanNode> nodes = ImmutableSet.builder();
        findSources(node, ImmutableSet.copyOf(nodeIds), nodes);
        return nodes.build();
    }

    private static void findSources(PlanNode node, Set<PlanNodeId> nodeIds, ImmutableSet.Builder<PlanNode> nodes)
    {
        if (nodeIds.contains(node.getId())) {
            nodes.add(node);
        }

        for (PlanNode source : node.getSources()) {
            nodes.addAll(findSources(source, nodeIds));
        }
    }

    private static void findRemoteSourceNodes(PlanNode node, Builder<RemoteSourceNode> builder)
    {
        // 遍历当前节点的子节点
        for (PlanNode source : node.getSources()) {
            findRemoteSourceNodes(source, builder);
        }

        if (node instanceof RemoteSourceNode) {
            builder.add((RemoteSourceNode) node);
        }
    }

    public PlanFragment withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new PlanFragment(
                id,
                root,
                variables,
                partitioning,
                tableScanSchedulingOrder,
                partitioningScheme.withBucketToPartition(bucketToPartition),
                stageExecutionDescriptor,
                outputTableWriterFragment,
                statsAndCosts,
                jsonRepresentation);
    }

    public PlanFragment withFixedLifespanScheduleGroupedExecution(List<PlanNodeId> capableTableScanNodes, int totalLifespans)
    {
        return new PlanFragment(
                id,
                root,
                variables,
                partitioning,
                tableScanSchedulingOrder,
                partitioningScheme,
                StageExecutionDescriptor.fixedLifespanScheduleGroupedExecution(capableTableScanNodes, totalLifespans),
                outputTableWriterFragment,
                statsAndCosts,
                jsonRepresentation);
    }

    public PlanFragment withDynamicLifespanScheduleGroupedExecution(List<PlanNodeId> capableTableScanNodes, int totalLifespans)
    {
        return new PlanFragment(
                id,
                root,
                variables,
                partitioning,
                tableScanSchedulingOrder,
                partitioningScheme,
                StageExecutionDescriptor.dynamicLifespanScheduleGroupedExecution(capableTableScanNodes, totalLifespans),
                outputTableWriterFragment,
                statsAndCosts,
                jsonRepresentation);
    }

    public PlanFragment withRecoverableGroupedExecution(List<PlanNodeId> capableTableScanNodes, int totalLifespans)
    {
        return new PlanFragment(
                id,
                root,
                variables,
                partitioning,
                tableScanSchedulingOrder,
                partitioningScheme,
                StageExecutionDescriptor.recoverableGroupedExecution(capableTableScanNodes, totalLifespans),
                outputTableWriterFragment,
                statsAndCosts,
                jsonRepresentation);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("partitioning", partitioning)
                .add("tableScanSchedulingOrder", tableScanSchedulingOrder)
                .add("partitionFunction", partitioningScheme)
                .toString();
    }
}
