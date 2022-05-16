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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.Session;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.getConcurrentLifespansPerNode;
import static com.facebook.presto.SystemSessionProperties.getMaxTasksPerStage;
import static com.facebook.presto.SystemSessionProperties.getWriterMinSize;
import static com.facebook.presto.SystemSessionProperties.isOptimizedScaleWriterProducerBuffer;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.spi.ConnectorId.isInternalSystemConnector;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * SectionExecution工厂，创建SectionExecution对象
 * 该类是一个非常重要的类。
 */
public class SectionExecutionFactory
{
    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeTaskMap nodeTaskMap;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final FailureDetector failureDetector;
    private final SplitSchedulerStats schedulerStats;
    private final NodeScheduler nodeScheduler;
    private final int splitBatchSize;

    @Inject
    public SectionExecutionFactory(
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            NodeTaskMap nodeTaskMap,
            @ForQueryExecution ExecutorService executor,
            @ForScheduler ScheduledExecutorService scheduledExecutor,
            FailureDetector failureDetector,
            SplitSchedulerStats schedulerStats,
            NodeScheduler nodeScheduler,
            QueryManagerConfig queryManagerConfig)
    {
        this(
                metadata,
                nodePartitioningManager,
                nodeTaskMap,
                executor,
                scheduledExecutor,
                failureDetector,
                schedulerStats,
                nodeScheduler,
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getScheduleSplitBatchSize());
    }

    public SectionExecutionFactory(
            Metadata metadata,
            NodePartitioningManager nodePartitioningManager,
            NodeTaskMap nodeTaskMap,
            ExecutorService executor,
            ScheduledExecutorService scheduledExecutor,
            FailureDetector failureDetector,
            SplitSchedulerStats schedulerStats,
            NodeScheduler nodeScheduler,
            int splitBatchSize)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.splitBatchSize = splitBatchSize;
    }

    /**
     * 创建SectionExecution
     * 返回一个SectionExecutions列表，用一个后序表示一棵树
     * returns a List of SectionExecutions in a postorder representation of the tree
     */
    public SectionExecution createSectionExecutions(
            Session session,
            StreamingPlanSection section,
            ExchangeLocationsConsumer locationsConsumer,
            Optional<int[]> bucketToPartition,
            OutputBuffers outputBuffers,
            boolean summarizeTaskInfo,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            int attemptId)
    {
        // Only fetch a distribution once per section to ensure all stages see the same machine assignments
        // 每个部分仅获取一次分发，以确保所有阶段都看到相同的机器分配
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();
        // 创建TableWriteInfo
        TableWriteInfo tableWriteInfo = createTableWriteInfo(section.getPlan(), metadata, session);
        // 将Stage的执行和调度器进行绑定
        List<StageExecutionAndScheduler> sectionStages = createStreamingLinkedStageExecutions(
                session,
                locationsConsumer,
                section.getPlan().withBucketToPartition(bucketToPartition),
                // TODO 这里会返回一个函数，如果有分区句柄，则构建NodePartitionMap
                partitioningHandle -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle)),
                tableWriteInfo,
                Optional.empty(),
                summarizeTaskInfo,
                remoteTaskFactory,
                splitSourceFactory,
                attemptId);
        // 获取最后一个StageExecutionAndScheduler
        // TODO 由上面createStreamingLinkedStageExecutions方法可知，rootStage是最后一个放进去的，这里把他给取出来
        StageExecutionAndScheduler rootStage = getLast(sectionStages);
        // 设置根阶段的outputBuffers
        rootStage.getStageExecution().setOutputBuffers(outputBuffers);
        // 创建SectionExecution对象，
        return new SectionExecution(rootStage, sectionStages);
    }

    /**
     * 创建流式的StageExecutionAndSchedulers
     *
     * 返回StageExecutionAndSchedulers列表树的后序表示形式
     * returns a List of StageExecutionAndSchedulers in a postorder representation of the tree
     */
    private List<StageExecutionAndScheduler> createStreamingLinkedStageExecutions(
            Session session,
            ExchangeLocationsConsumer parent,
            StreamingSubPlan plan,
            // TODO 分区函数，该函数包含了，桶、分区、节点间的对应关系
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            TableWriteInfo tableWriteInfo,
            Optional<SqlStageExecution> parentStageExecution,
            boolean summarizeTaskInfo,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            int attemptId)
    {
        // 创建StageExecutionAndScheduler列表
        ImmutableList.Builder<StageExecutionAndScheduler> stageExecutionAndSchedulers = ImmutableList.builder();

        // 创建StageId
        PlanFragmentId fragmentId = plan.getFragment().getId();
        StageId stageId = new StageId(session.getQueryId(), fragmentId.getId());
        // 创建SqlStageExecution对象
        // 在阶段执行的时候，会传入该对象
        SqlStageExecution stageExecution = createSqlStageExecution(
                new StageExecutionId(stageId, attemptId),
                plan.getFragment(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                executor,
                failureDetector,
                schedulerStats,
                tableWriteInfo);

        // 分区句柄
        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        // 查询RemoteSourceNode节点列表
        List<RemoteSourceNode> remoteSourceNodes = plan.getFragment().getRemoteSourceNodes();
        // 获取桶到分区的映射关系
        Optional<int[]> bucketToPartition = getBucketToPartition(partitioningHandle, partitioningCache, plan.getFragment().getRoot(), remoteSourceNodes);

        // create child stages
        // 创建子阶段
        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        // 遍历流式子计划的子计划
        for (StreamingSubPlan stagePlan : plan.getChildren()) {
            // TODO 递归调用当前方法，创建不同阶段的调度器
            List<StageExecutionAndScheduler> subTree = createStreamingLinkedStageExecutions(
                    session,
                    // TODO 添加数据交换的位置信息
                    stageExecution::addExchangeLocations,
                    stagePlan.withBucketToPartition(bucketToPartition),
                    partitioningCache,
                    tableWriteInfo,
                    Optional.of(stageExecution),
                    summarizeTaskInfo,
                    remoteTaskFactory,
                    splitSourceFactory,
                    attemptId);
            stageExecutionAndSchedulers.addAll(subTree);
            // 其中getLast拿的就是outputStage，
            childStagesBuilder.add(getLast(subTree).getStageExecution());
        }
        // 子节点
        Set<SqlStageExecution> childStageExecutions = childStagesBuilder.build();
        // 添加状态改变监听器，当完成之后，取消远程任务
        stageExecution.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                childStageExecutions.forEach(SqlStageExecution::cancel);
            }
        });

        // 创建StageLinkage
        StageLinkage stageLinkage = new StageLinkage(fragmentId, parent, childStageExecutions);
        // TODO 创建StageScheduler, 不同的阶段创建了不同的调度器， 这个应该是Root StageScheduler
        // 因为上面会递归调用当前方法，所以这里会为【不同的阶段创建不同的调度器】
        StageScheduler stageScheduler = createStageScheduler(
                splitSourceFactory,
                session,
                plan,
                partitioningCache,
                parentStageExecution,
                stageId,
                stageExecution,
                partitioningHandle,
                tableWriteInfo,
                childStageExecutions);
        // 创建StageExecutionAndScheduler并添加至列表
        stageExecutionAndSchedulers.add(new StageExecutionAndScheduler(
                stageExecution,
                stageLinkage,
                stageScheduler));

        return stageExecutionAndSchedulers.build();
    }

    /**
     * 创建Stage调度器，不同的阶段创建的调度器不同
     * 。
     * 其中single 和 fixed 均是随机选择
     * TODO remote source 指的是，worker节点上的split，而不是Connector上的split（split source）
     */
    private StageScheduler createStageScheduler(
            SplitSourceFactory splitSourceFactory,
            Session session,
            StreamingSubPlan plan,
            // 该函数会返回NodePartitionMap对象，可以获取 桶、分区、节点之间的对应关系
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            Optional<SqlStageExecution> parentStageExecution,
            StageId stageId,
            SqlStageExecution stageExecution,
            PartitioningHandle partitioningHandle,
            TableWriteInfo tableWriteInfo,
            Set<SqlStageExecution> childStageExecutions)
    {
        // TODO 创建SplitSource，其中：key：计划ID，value：数据库的数据分片
        Map<PlanNodeId, SplitSource> splitSources = splitSourceFactory.createSplitSources(plan.getFragment(), session, tableWriteInfo);
        // 每个阶段的最大任务数
        int maxTasksPerStage = getMaxTasksPerStage(session);
        // TODO SOURCE分布，表示查询数据源的操作，使用SourcePartitionedScheduler进行调度
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            // nodes are selected dynamically based on the constraints of the splits and the system load
            // 根据split约束和系统负载动态选择节点
            Map.Entry<PlanNodeId, SplitSource> entry = getOnlyElement(splitSources.entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            ConnectorId connectorId = splitSource.getConnectorId();
            if (isInternalSystemConnector(connectorId)) {
                connectorId = null;
            }

            // 创建节点选择器 TopologyAwareNodeSelector or SimpleNodeSelector
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, connectorId, maxTasksPerStage);
            // 创建Split安放策略，是动态Split安置策略，在调度的时候会使用到
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

            checkArgument(!plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution());
            // TODO 创建SourcePartitionedScheduler作为StageScheduler
            return newSourcePartitionedSchedulerAsStageScheduler(stageExecution, planNodeId, splitSource, placementPolicy, splitBatchSize);
        }
        // TODO 使用ScaledWriterScheduler调度SCALED_WRITER_DISTRIBUTION阶段任务
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStageExecutions.stream()
                    .map(SqlStageExecution::getAllTasks)
                    .flatMap(Collection::stream)
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Supplier<Collection<TaskStatus>> writerTasksProvider = () -> stageExecution.getAllTasks().stream()
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stageExecution,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(session, null),
                    scheduledExecutor,
                    getWriterMinSize(session),
                    isOptimizedScaleWriterProducerBuffer(session));
            whenAllStages(childStageExecutions, StageExecutionState::isDone)
                    .addListener(scheduler::finish, directExecutor());
            return scheduler;
        }
        // TODO 其他的分布方式，SINGLE、FIXED
        else {
            // TODO 如果当前stage需要读取本地数据源，创建 FixedSourcePartitionedScheduler
            //  为啥不为空就是读取本地数据源呢？因为：splitSources切片本身就是去读取数据库服务的，split是对数据库自身对数据的切分。
            // splitSources应该是读取Connector的split，表示当前调度器可能需要
            if (!splitSources.isEmpty()) {
                // contains local source
                List<PlanNodeId> schedulingOrder = plan.getFragment().getTableScanSchedulingOrder();
                ConnectorId connectorId = partitioningHandle.getConnectorId().orElseThrow(IllegalStateException::new);
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                boolean groupedExecutionForStage = plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution();
                if (groupedExecutionForStage) {
                    connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                    checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                }
                else {
                    connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                }

                // 桶到节点的映射关系
                BucketNodeMap bucketNodeMap;
                // 本阶段涉及到的节点列表
                List<InternalNode> stageNodeList;
                // 如果ExchangeType是广播
                if (plan.getFragment().getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    // no non-replicated remote source
                    boolean dynamicLifespanSchedule = plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule();
                    // 获取桶和节点的映射
                    bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule);

                    // verify execution is consistent with planner's decision on dynamic lifespan schedule
                    verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                    if (bucketNodeMap.hasInitialMap()) {
                        stageNodeList = bucketNodeMap.getBucketToNode().get().stream()
                                .distinct()
                                .collect(toImmutableList());
                    }
                    else {
                        stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(session, connectorId).selectRandomNodes(maxTasksPerStage));
                    }
                }
                else {
                    // cannot use dynamic lifespan schedule
                    verify(!plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule());

                    // remote source requires nodePartitionMap
                    // 节点到分区的映射，remote source要求有nodePartitionMap
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                    // 是否分组执行
                    if (groupedExecutionForStage) {
                        checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                    }
                    stageNodeList = nodePartitionMap.getPartitionToNode();
                    bucketNodeMap = nodePartitionMap.asBucketNodeMap();
                }

                // 创建调度器
                FixedSourcePartitionedScheduler fixedSourcePartitionedScheduler = new FixedSourcePartitionedScheduler(
                        stageExecution,
                        splitSources,
                        plan.getFragment().getStageExecutionDescriptor(),
                        schedulingOrder,
                        stageNodeList,
                        bucketNodeMap,
                        splitBatchSize,
                        getConcurrentLifespansPerNode(session),
                        nodeScheduler.createNodeSelector(session, connectorId),
                        connectorPartitionHandles);
                if (plan.getFragment().getStageExecutionDescriptor().isRecoverableGroupedExecution()) {
                    stageExecution.registerStageTaskRecoveryCallback(taskId -> {
                        checkArgument(taskId.getStageExecutionId().getStageId().equals(stageId), "The task did not execute this stage");
                        checkArgument(parentStageExecution.isPresent(), "Parent stage execution must exist");
                        checkArgument(parentStageExecution.get().getAllTasks().size() == 1, "Parent stage should only have one task for recoverable grouped execution");

                        parentStageExecution.get().removeRemoteSourceIfSingleTaskStage(taskId);
                        fixedSourcePartitionedScheduler.recover(taskId);
                    });
                }
                return fixedSourcePartitionedScheduler;
            }

            // TODO 如果所有的sources都是remote source，则创建FixedCountScheduler. 这里就
            else {
                // all sources are remote
                // 这里属于所有的source都是remote source
                NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning());
                // 分区和节点的映射
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                return new FixedCountScheduler(stageExecution, partitionToNode);
            }
        }
    }

    /**
     *
     * @param partitioningHandle
     * @param partitioningCache
     * @param fragmentRoot
     * @param remoteSourceNodes
     * @return
     */
    private static Optional<int[]> getBucketToPartition(
            PartitioningHandle partitioningHandle,
            Function<PartitioningHandle, NodePartitionMap> partitioningCache,
            PlanNode fragmentRoot,
            List<RemoteSourceNode> remoteSourceNodes)
    {
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION) || partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            return Optional.of(new int[1]);
        }
        else if (PlanNodeSearcher.searchFrom(fragmentRoot).where(node -> node instanceof TableScanNode).findFirst().isPresent()) {
            if (remoteSourceNodes.stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                return Optional.empty();
            }
            else {
                // remote source requires nodePartitionMap
                NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
                return Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }
        else {
            NodePartitionMap nodePartitionMap = partitioningCache.apply(partitioningHandle);
            List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
            // todo this should asynchronously wait a standard timeout period before failing
            checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
            return Optional.of(nodePartitionMap.getBucketToPartition());
        }
    }

    private static ListenableFuture<?> whenAllStages(Collection<SqlStageExecution> stageExecutions, Predicate<StageExecutionState> predicate)
    {
        checkArgument(!stageExecutions.isEmpty(), "stageExecutions is empty");
        Set<StageExecutionId> stageIds = newConcurrentHashSet(stageExecutions.stream()
                .map(SqlStageExecution::getStageExecutionId)
                .collect(toSet()));
        SettableFuture<?> future = SettableFuture.create();

        for (SqlStageExecution stage : stageExecutions) {
            stage.addStateChangeListener(state -> {
                if (predicate.test(state) && stageIds.remove(stage.getStageExecutionId()) && stageIds.isEmpty()) {
                    future.set(null);
                }
            });
        }

        return future;
    }
}
