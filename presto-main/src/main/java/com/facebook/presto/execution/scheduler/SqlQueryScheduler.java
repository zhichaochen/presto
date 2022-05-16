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

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.Session;
import com.facebook.presto.execution.BasicStageExecutionStats;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.airlift.concurrent.MoreFutures.whenAnyComplete;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.SystemSessionProperties.getMaxConcurrentMaterializations;
import static com.facebook.presto.SystemSessionProperties.getMaxStageRetries;
import static com.facebook.presto.SystemSessionProperties.isRuntimeOptimizerEnabled;
import static com.facebook.presto.execution.BasicStageExecutionStats.aggregateBasicStageStats;
import static com.facebook.presto.execution.SqlStageExecution.RECOVERABLE_ERROR_CODES;
import static com.facebook.presto.execution.StageExecutionInfo.unscheduledExecutionInfo;
import static com.facebook.presto.execution.StageExecutionState.CANCELED;
import static com.facebook.presto.execution.StageExecutionState.FAILED;
import static com.facebook.presto.execution.StageExecutionState.FINISHED;
import static com.facebook.presto.execution.StageExecutionState.RUNNING;
import static com.facebook.presto.execution.StageExecutionState.SCHEDULED;
import static com.facebook.presto.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.execution.buffer.OutputBuffers.createDiscardingOutputBuffers;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.scheduler.StreamingPlanSection.extractStreamingSections;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.planner.PlanFragmenter.ROOT_FRAGMENT_ID;
import static com.facebook.presto.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonFragmentPlan;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * SQL查询调度器
 *
 * 会构建SqlStage执行器
 */
public class SqlQueryScheduler
        implements SqlQuerySchedulerInterface
{
    private static final Logger log = Logger.get(SqlQueryScheduler.class);

    private final LocationFactory locationFactory;
    private final ExecutionPolicy executionPolicy;
    private final ExecutorService executor;
    private final SplitSchedulerStats schedulerStats;
    private final SectionExecutionFactory sectionExecutionFactory;
    private final RemoteTaskFactory remoteTaskFactory;
    private final SplitSourceFactory splitSourceFactory;
    private final InternalNodeManager nodeManager;

    private final Session session;
    private final QueryStateMachine queryStateMachine;
    private final AtomicReference<SubPlan> plan = new AtomicReference<>();

    // The following fields are required for adaptive optimization in runtime.
    private final FunctionAndTypeManager functionAndTypeManager;
    private final List<PlanOptimizer> runtimePlanOptimizers;
    private final WarningCollector warningCollector;
    private final PlanNodeIdAllocator idAllocator;
    private final PlanVariableAllocator variableAllocator;
    private final Set<StageId> runtimeOptimizedStages = Collections.synchronizedSet(new HashSet<>());
    private final PlanChecker planChecker;
    private final Metadata metadata;
    private final SqlParser sqlParser;

    private final StreamingPlanSection sectionedPlan;
    private final boolean summarizeTaskInfo;
    private final int maxConcurrentMaterializations;
    private final int maxStageRetries;

    // 每个阶段的
    private final Map<StageId, List<SectionExecution>> sectionExecutions = new ConcurrentHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean scheduling = new AtomicBoolean(); // 是否正在调度中
    private final AtomicInteger retriedSections = new AtomicInteger();

    /**
     * 创建Sql查询调度器
     * @return
     */
    public static SqlQueryScheduler createSqlQueryScheduler(
            LocationFactory locationFactory,
            ExecutionPolicy executionPolicy,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            SectionExecutionFactory sectionExecutionFactory,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            InternalNodeManager nodeManager,
            Session session,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            boolean summarizeTaskInfo,
            FunctionAndTypeManager functionAndTypeManager,
            List<PlanOptimizer> runtimePlanOptimizers,
            WarningCollector warningCollector,
            PlanNodeIdAllocator idAllocator,
            PlanVariableAllocator variableAllocator,
            PlanChecker planChecker,
            Metadata metadata,
            SqlParser sqlParser)
    {
        SqlQueryScheduler sqlQueryScheduler = new SqlQueryScheduler(
                locationFactory,
                executionPolicy,
                executor,
                schedulerStats,
                sectionExecutionFactory,
                remoteTaskFactory,
                splitSourceFactory,
                nodeManager,
                session,
                queryStateMachine,
                plan,
                summarizeTaskInfo,
                functionAndTypeManager,
                runtimePlanOptimizers,
                warningCollector,
                idAllocator,
                variableAllocator,
                planChecker,
                metadata,
                sqlParser);
        sqlQueryScheduler.initialize();
        return sqlQueryScheduler;
    }

    private SqlQueryScheduler(
            LocationFactory locationFactory,
            ExecutionPolicy executionPolicy,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            SectionExecutionFactory sectionExecutionFactory,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            InternalNodeManager nodeManager,
            Session session,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            boolean summarizeTaskInfo,
            FunctionAndTypeManager functionAndTypeManager,
            List<PlanOptimizer> runtimePlanOptimizers,
            WarningCollector warningCollector,
            PlanNodeIdAllocator idAllocator,
            PlanVariableAllocator variableAllocator,
            PlanChecker planChecker,
            Metadata metadata,
            SqlParser sqlParser)
    {
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.sectionExecutionFactory = requireNonNull(sectionExecutionFactory, "sectionExecutionFactory is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.session = requireNonNull(session, "session is null");
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.runtimePlanOptimizers = requireNonNull(runtimePlanOptimizers, "runtimePlanOptimizers is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.planChecker = requireNonNull(planChecker, "planChecker is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.plan.compareAndSet(null, requireNonNull(plan, "plan is null"));
        // TODO 提取流式分段的计划。这是创建分布式执行计划中很重要的一步
        this.sectionedPlan = extractStreamingSections(plan);
        this.summarizeTaskInfo = summarizeTaskInfo;
        // 物化的最大并发数
        this.maxConcurrentMaterializations = getMaxConcurrentMaterializations(session);
        // 每个阶段最大的重试次数
        this.maxStageRetries = getMaxStageRetries(session);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        // 当查询完成或某个阶段完成时，尝试将查询转换为“最终查询信息就绪”
        queryStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                queryStateMachine.updateQueryInfo(Optional.of(getStageInfo()));
            }
        });
    }

    /**
     * 启动sql查询调度器
     */
    @Override
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            startScheduling();
        }
    }

    /**
     * 开始调度
     */
    private void startScheduling()
    {
        // still scheduling the previous batch of stages
        if (scheduling.get()) {
            return;
        }
        executor.submit(this::schedule);
    }

    /**
     * 调度分布式任务
     */
    private void schedule()
    {
        // 如果正在调度前一批Stage，则退出
        if (!scheduling.compareAndSet(false, true)) {
            // still scheduling the previous batch of stages
            return;
        }

        // 待调度的
        List<StageExecutionAndScheduler> scheduledStageExecutions = new ArrayList<>();

        // 设置当前线程名
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            Set<StageId> completedStages = new HashSet<>();

            List<ExecutionSchedule> executionSchedules = new LinkedList<>();

            // 如果当前线程没有被打断，则一直轮询
            while (!Thread.currentThread().isInterrupted()) {
                // remove finished section
                // 遍历executionSchedules，删除已完成的部分
                executionSchedules.removeIf(ExecutionSchedule::isFinished);

                // try to pull more section that are ready to be run
                // 尝试拉更多就绪的部分（先序遍历执行计树，并限制最大并发数）
                // TODO 在创建当前对象SqlQueryScheduler的时候对其进行了初始化
                List<StreamingPlanSection> sectionsReadyForExecution = getSectionsReadyForExecution();

                // all finished
                // 如果所有的已经完成，跳出轮训
                if (sectionsReadyForExecution.isEmpty() && executionSchedules.isEmpty()) {
                    break;
                }

                // Apply runtime CBO on the ready sections before creating SectionExecutions.
                // 在创建SectionExecutions之前，在就绪的部分上应用运行时CBO。
                // TODO 在创建StreamingPlanSection对象的时候，会为不同的阶段创建不同的调度器
                List<SectionExecution> sectionExecutions = createStageExecutions(sectionsReadyForExecution.stream()
                        // 基于成本优化CBO
                        .map(this::tryCostBasedOptimize)
                        .collect(toImmutableList()));
                // 如果查询状态机的状态是已完成，则终止就绪的部分，并退出循环
                if (queryStateMachine.isDone()) {
                    sectionExecutions.forEach(SectionExecution::abort);
                    break;
                }

                // 添加SectionExecution所有StageExecutionAndScheduler到scheduledStageExecutions中，
                sectionExecutions.forEach(sectionExecution -> scheduledStageExecutions.addAll(sectionExecution.getSectionStages()));
                // 为每个部分创建相应的执行策略
                sectionExecutions.stream()
                        .map(SectionExecution::getSectionStages)
                        .map(executionPolicy::createExecutionSchedule)
                        .forEach(executionSchedules::add);

                // 如果待调度的部分不为空且没有执行完成，则进入轮询
                while (!executionSchedules.isEmpty() && executionSchedules.stream().noneMatch(ExecutionSchedule::isFinished)) {
                    List<ListenableFuture<?>> blockedStages = new ArrayList<>();

                    // 获取所有待跳度的StageExecutionAndScheduler列表
                    List<StageExecutionAndScheduler> executionsToSchedule = executionSchedules.stream()
                            .flatMap(schedule -> schedule.getStagesToSchedule().stream())
                            .collect(toImmutableList());

                    // 遍历所有待调度的stage
                    for (StageExecutionAndScheduler executionAndScheduler : executionsToSchedule) {
                        // 状态机设置为调度中
                        executionAndScheduler.getStageExecution().beginScheduling();

                        // perform some scheduling work
                        // TODO 获取Stage调度器并调度，不同阶段对应不同的调度器
                        ScheduleResult result = executionAndScheduler.getStageScheduler()
                                .schedule();

                        // modify parent and children based on the results of the scheduling
                        // 如果调度完成，则更改父级和子级stage的状态机的状态
                        if (result.isFinished()) {
                            executionAndScheduler.getStageExecution().schedulingComplete();
                        }
                        // 如果stage被阻塞了，则加入阻塞列表
                        else if (!result.getBlocked().isDone()) {
                            blockedStages.add(result.getBlocked());
                        }
                        // TODO 根据当前stage调度结果，设置父stage的shuffle追踪地址（用来进行stage间的数据交换）
                        // TODO 这里非常重要，给下一个Stage设置查询地址
                        executionAndScheduler.getStageLinkage()
                                .processScheduleResults(executionAndScheduler.getStageExecution().getState(), result.getNewTasks());
                        // 更新已经调度的分区个数
                        schedulerStats.getSplitsScheduledPerIteration().add(result.getSplitsScheduled());
                        // 更新阻塞的情况
                        if (result.getBlockedReason().isPresent()) {
                            switch (result.getBlockedReason().get()) {
                                case WRITER_SCALING:
                                    // no-op
                                    break;
                                case WAITING_FOR_SOURCE:
                                    schedulerStats.getWaitingForSource().update(1);
                                    break;
                                case SPLIT_QUEUES_FULL:
                                    schedulerStats.getSplitQueuesFull().update(1);
                                    break;
                                case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                                    schedulerStats.getMixedSplitQueuesFullAndWaitingForSource().update(1);
                                    break;
                                case NO_ACTIVE_DRIVER_GROUP:
                                    schedulerStats.getNoActiveDriverGroup().update(1);
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                            }
                        }
                    }

                    // make sure to update stage linkage at least once per loop to catch async state changes (e.g., partial cancel)
                    // 确保每个循环至少更新一次stage linkage，以捕获异步状态更改（例如，部分取消）
                    // TODO 遍历已调度在执行中的stage，如果有stage已执行完成，则更新其父stage的shuffle追踪地址（从而可以启动下一阶段的stage执行）
                    boolean stageFinishedExecution = false;
                    for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                        SqlStageExecution stageExecution = stageExecutionAndScheduler.getStageExecution();
                        // 当前阶段ID
                        StageId stageId = stageExecution.getStageExecutionId().getStageId();
                        // 判断当前阶段是否已经调度完成
                        if (!completedStages.contains(stageId) && stageExecution.getState().isDone()) {
                            // 处理【阶段】的调度结果, 参考：SqlStageExecution#addExchangeLocations
                            // TODO 对于非源头的stage，消费的是上游提供的remote split，而不是某个 connector split,
                            //   所以需要在上游Stage调度时，获取其task的摆放位置，以此创建Remote Split，喂给下游 stage
                            //   这样每个 task 就都知道去哪些上游 task pull 数据了.
                            stageExecutionAndScheduler.getStageLinkage()
                                    .processScheduleResults(stageExecution.getState(), ImmutableSet.of());
                            // 添加至完成的阶段列表
                            completedStages.add(stageId);
                            // 设置阶段已经完成
                            stageFinishedExecution = true;
                        }
                    }

                    // if any stage has just finished execution try to pull more sections for scheduling
                    // 如果任何阶段刚刚完成执行，则终止循环，以尝试拉取更多sections以进行调度
                    if (stageFinishedExecution) {
                        break;
                    }

                    // wait for a state change and then schedule again
                    // 等待状态更改，然后再次调度
                    if (!blockedStages.isEmpty()) {
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                        }
                        for (ListenableFuture<?> blockedStage : blockedStages) {
                            blockedStage.cancel(true);
                        }
                    }
                }
            }

            // 所有阶段已经调度完毕，验证调度状态是否正确
            for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                StageExecutionState state = stageExecutionAndScheduler.getStageExecution().getState();
                if (state != SCHEDULED && state != RUNNING && !state.isDone()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage execution %s is in state %s", stageExecutionAndScheduler.getStageExecution().getStageExecutionId(), state));
                }
            }

            // 调度完毕，更新调度中为false
            scheduling.set(false);

            // 如果还有准备好的部分，重新开始跳度
            if (!getSectionsReadyForExecution().isEmpty()) {
                startScheduling();
            }
        }
        catch (Throwable t) {
            scheduling.set(false);
            queryStateMachine.transitionToFailed(t);
            throw t;
        }
        finally {
            // 关闭阶段调度器
            RuntimeException closeError = new RuntimeException();
            for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                try {
                    stageExecutionAndScheduler.getStageScheduler().close();
                }
                catch (Throwable t) {
                    queryStateMachine.transitionToFailed(t);
                    // Self-suppression not permitted
                    if (closeError != t) {
                        closeError.addSuppressed(t);
                    }
                }
            }
            if (closeError.getSuppressed().length > 0) {
                throw closeError;
            }
        }
    }

    /**
     * 基于成本优化
     * 用于调用基于运行时成本的优化器的通用实用程序函数。
     * （目前只有一个计划优化器确定是否应交换JoinNode的探测和构建端
     * 基于临时表的统计信息，临时表包含已完成子部分的物化交换输出）
     *
     * A general purpose utility function to invoke runtime cost-based optimizer.
     * (right now there is only one plan optimizer which determines if the probe and build side of a JoinNode should be swapped
     * based on the statistics of the temporary table holding materialized exchange outputs from finished children sections)
     */
    private StreamingPlanSection tryCostBasedOptimize(StreamingPlanSection section)
    {
        // no need to do runtime optimization if no materialized exchange data is utilized by the section.
        // 如果该部分未使用物化的exchange数据，则无需执行运行时优化。
        if (!isRuntimeOptimizerEnabled(session) || section.getChildren().isEmpty()) {
            return section;
        }

        // Apply runtime optimization on each StreamingSubPlan's fragment
        // 应用运行时优化在每个StreamingSubPlan的段上
        Map<PlanFragment, PlanFragment> oldToNewFragment = new HashMap<>();
        // 深度优先遍历
        stream(forTree(StreamingSubPlan::getChildren).depthFirstPreOrder(section.getPlan()))
                .forEach(currentSubPlan -> {
                    // 执行运行时优化
                    Optional<PlanFragment> newPlanFragment = performRuntimeOptimizations(currentSubPlan);
                    if (newPlanFragment.isPresent()) {
                        planChecker.validatePlanFragment(newPlanFragment.get().getRoot(), session, metadata, sqlParser, variableAllocator.getTypes(), warningCollector);
                        oldToNewFragment.put(currentSubPlan.getFragment(), newPlanFragment.get());
                    }
                });

        // Early exit when no stage's fragment is changed
        // 未更改任何阶段的片段时提前退出
        if (oldToNewFragment.isEmpty()) {
            return section;
        }

        oldToNewFragment.forEach((oldFragment, newFragment) -> runtimeOptimizedStages.add(getStageId(oldFragment.getId())));

        // Update SubPlan so that getStageInfo will reflect the latest optimized plan when query is finished.
        // 更新子计划，以便在查询完成时getStageInfo将反映最新的优化计划。
        updatePlan(oldToNewFragment);

        log.debug("Invoked CBO during runtime, optimized stage IDs: " + oldToNewFragment.keySet().stream()
                .map(PlanFragment::getId)
                .map(PlanFragmentId::toString)
                .collect(Collectors.joining(", ")));
        // 重写其子计划
        return new StreamingPlanSection(rewriteStreamingSubPlan(section.getPlan(), oldToNewFragment), section.getChildren());
    }

    /**
     * 执行运行时优化
     * @param subPlan
     * @return
     */
    private Optional<PlanFragment> performRuntimeOptimizations(StreamingSubPlan subPlan)
    {
        PlanFragment fragment = subPlan.getFragment();
        PlanNode newRoot = fragment.getRoot();
        // 遍历每个优化器进行优化
        for (PlanOptimizer optimizer : runtimePlanOptimizers) {
            newRoot = optimizer.optimize(newRoot, session, variableAllocator.getTypes(), variableAllocator, idAllocator, warningCollector);
        }
        if (newRoot != fragment.getRoot()) {
            return Optional.of(
                    // The partitioningScheme should stay the same
                    // even if the root's outputVariable layout is changed.
                    new PlanFragment(
                            fragment.getId(),
                            newRoot,
                            fragment.getVariables(),
                            fragment.getPartitioning(),
                            scheduleOrder(newRoot),
                            fragment.getPartitioningScheme(),
                            fragment.getStageExecutionDescriptor(),
                            fragment.isOutputTableWriterFragment(),
                            fragment.getStatsAndCosts(),
                            Optional.of(jsonFragmentPlan(newRoot, fragment.getVariables(), functionAndTypeManager, session))));
        }
        return Optional.empty();
    }

    private void updatePlan(Map<PlanFragment, PlanFragment> oldToNewFragments)
    {
        plan.getAndUpdate(value -> rewritePlan(value, oldToNewFragments));
    }

    private SubPlan rewritePlan(SubPlan root, Map<PlanFragment, PlanFragment> oldToNewFragments)
    {
        ImmutableList.Builder<SubPlan> children = ImmutableList.builder();
        for (SubPlan child : root.getChildren()) {
            children.add(rewritePlan(child, oldToNewFragments));
        }
        if (oldToNewFragments.containsKey(root.getFragment())) {
            return new SubPlan(oldToNewFragments.get(root.getFragment()), children.build());
        }
        else {
            return new SubPlan(root.getFragment(), children.build());
        }
    }

    private StreamingSubPlan rewriteStreamingSubPlan(StreamingSubPlan root, Map<PlanFragment, PlanFragment> oldToNewFragment)
    {
        ImmutableList.Builder<StreamingSubPlan> childrenPlans = ImmutableList.builder();
        for (StreamingSubPlan child : root.getChildren()) {
            childrenPlans.add(rewriteStreamingSubPlan(child, oldToNewFragment));
        }
        if (oldToNewFragment.containsKey(root.getFragment())) {
            return new StreamingSubPlan(oldToNewFragment.get(root.getFragment()), childrenPlans.build());
        }
        else {
            return new StreamingSubPlan(root.getFragment(), childrenPlans.build());
        }
    }

    /**
     * 获取准备好了，可以执行的部分
     * @return
     */
    private List<StreamingPlanSection> getSectionsReadyForExecution()
    {
        // 查询正在运行中的片段个数
        long runningPlanSections =
                // 先序遍历sectionedPlan
                stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                        // 获取最后的SectionExecution的FragmentID
                        .map(section -> getLatestSectionExecution(getStageId(section.getPlan().getFragment().getId())))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(SectionExecution::isRunning)
                        .count();
        // 查询可以运行的部分
        return stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                // get all sections ready for execution
                // 获取所有准备执行的部分
                .filter(this::isReadyForExecution)
                .limit(maxConcurrentMaterializations - runningPlanSections)
                .collect(toImmutableList());
    }

    /**
     * 是否准备执行
     * @param section
     * @return
     */
    private boolean isReadyForExecution(StreamingPlanSection section)
    {
        // 获取最新的SectionExecution
        Optional<SectionExecution> sectionExecution = getLatestSectionExecution(getStageId(section.getPlan().getFragment().getId()));
        // 该SectionExecution正在运行或者已经完成，则返回false
        if (sectionExecution.isPresent() && (sectionExecution.get().isRunning() || sectionExecution.get().isFinished())) {
            // already scheduled
            return false;
        }
        // 判断其子部分，是否有正在运行的或者完成的
        for (StreamingPlanSection child : section.getChildren()) {
            Optional<SectionExecution> childSectionExecution = getLatestSectionExecution(getStageId(child.getPlan().getFragment().getId()));
            if (!childSectionExecution.isPresent() || !childSectionExecution.get().isFinished()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 获取最后的SectionExecution
     * @param stageId
     * @return
     */
    private Optional<SectionExecution> getLatestSectionExecution(StageId stageId)
    {
        List<SectionExecution> sectionExecutions = this.sectionExecutions.get(stageId);
        if (sectionExecutions == null || sectionExecutions.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getLast(sectionExecutions));
    }

    private StageId getStageId(PlanFragmentId fragmentId)
    {
        return new StageId(session.getQueryId(), fragmentId.getId());
    }

    /**
     * 创建SectionExecution，每个是一个可调度的部分
     * @param sections
     * @return
     */
    private List<SectionExecution> createStageExecutions(List<StreamingPlanSection> sections)
    {
        ImmutableList.Builder<SectionExecution> result = ImmutableList.builder();
        // 遍历所有流式计划
        for (StreamingPlanSection section : sections) {
            // 创建阶段ID
            StageId sectionId = getStageId(section.getPlan().getFragment().getId());
            // 如果不存在，则创建一个CopyOnWriteArrayList类型的新集合
            List<SectionExecution> attempts = sectionExecutions.computeIfAbsent(sectionId, (ignored) -> new CopyOnWriteArrayList<>());

            // sectionExecutions only get created when they are about to be scheduled, so there should
            // never be a non-failed SectionExecution for a section that's ready for execution
            verify(attempts.isEmpty() || getLast(attempts).isFailed(), "Non-failed sectionExecutions already exists");

            // 计划片段
            PlanFragment sectionRootFragment = section.getPlan().getFragment();

            Optional<int[]> bucketToPartition; // bucket和分区的映射
            OutputBuffers outputBuffers;
            ExchangeLocationsConsumer locationsConsumer;
            // 是否是Root Fragment，需要更新outputBuffers，其中locationsConsumer定义了更新逻辑
            // TODO Root Fragment 表示 output Fragment
            if (isRootFragment(sectionRootFragment)) {
                // 如果是根节点
                bucketToPartition = Optional.of(new int[1]);
                // 创建一个空的outputBuffers对象
                outputBuffers = createInitialEmptyOutputBuffers(sectionRootFragment.getPartitioningScheme().getPartitioning().getHandle())
                        .withBuffer(new OutputBufferId(0), BROADCAST_PARTITION_ID)
                        .withNoMoreBufferIds();
                // 获取outputBuffers的第一个元素
                OutputBufferId rootBufferId = getOnlyElement(outputBuffers.getBuffers().keySet());
                //  TODO 定义ExchangeLocationsConsumer消费逻辑 ，
                //   当一个stage调度完成之后， 在处理调度结果的时候，会创建该方法。
                locationsConsumer = (fragmentId, tasks, noMoreExchangeLocations) ->
                        updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations);
            }
            // 如果不是根节点，为啥要这样呢？
            else {
                bucketToPartition = Optional.empty();
                outputBuffers = createDiscardingOutputBuffers();
                // 不需要更新任何位置
                locationsConsumer = (fragmentId, tasks, noMoreExchangeLocations) -> {};
            }

            int attemptId = attempts.size();
            // 创建分段执行对象
            // 这里会做不少事情，比如：
            SectionExecution sectionExecution = sectionExecutionFactory.createSectionExecutions(
                    session,
                    section,
                    locationsConsumer,
                    bucketToPartition,
                    outputBuffers,
                    summarizeTaskInfo,
                    remoteTaskFactory,
                    splitSourceFactory,
                    attemptId);

            // 添加状态改变监听器
            addStateChangeListeners(sectionExecution);
            // 加入集合
            attempts.add(sectionExecution);
            result.add(sectionExecution);
        }
        return result.build();
    }

    /**
     * 更新缓存输出URL
     * @param queryStateMachine : 查询状态机
     * @param rootBufferId ：根缓存ID，output 阶段的缓存ID
     * @param tasks ： 远程任务
     * @param noMoreExchangeLocations ：是否还有更多的
     */
    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        // 缓存位置
        Map<URI, TaskId> bufferLocations = tasks.stream()
                .collect(toImmutableMap(
                        task -> getBufferLocation(task, rootBufferId),
                        RemoteTask::getTaskId));
        queryStateMachine.updateOutputLocations(bufferLocations, noMoreExchangeLocations);
    }

    /**
     * 获取
     * @param remoteTask
     * @param rootBufferId
     * @return
     */
    private static URI getBufferLocation(RemoteTask remoteTask, OutputBufferId rootBufferId)
    {
        URI location = remoteTask.getTaskStatus().getSelf();
        return uriBuilderFrom(location).appendPath("results").appendPath(rootBufferId.toString()).build();
    }

    private void addStateChangeListeners(SectionExecution sectionExecution)
    {
        for (StageExecutionAndScheduler stageExecutionAndScheduler : sectionExecution.getSectionStages()) {
            SqlStageExecution stageExecution = stageExecutionAndScheduler.getStageExecution();
            if (isRootFragment(stageExecution.getFragment())) {
                stageExecution.addStateChangeListener(state -> {
                    if (state == FINISHED) {
                        queryStateMachine.transitionToFinishing();
                    }
                    else if (state == CANCELED) {
                        // output stage was canceled
                        queryStateMachine.transitionToCanceled();
                    }
                });
            }
            stageExecution.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }

                if (state == FAILED) {
                    ExecutionFailureInfo failureInfo = stageExecution.getStageExecutionInfo().getFailureCause()
                            .orElseThrow(() -> new VerifyException(format("stage execution failed, but the failure info is missing: %s", stageExecution.getStageExecutionId())));
                    Exception failureException = failureInfo.toException();

                    boolean isRootSection = isRootFragment(sectionExecution.getRootStage().getStageExecution().getFragment());
                    // root section directly streams the results to the user, cannot be retried
                    if (isRootSection) {
                        queryStateMachine.transitionToFailed(failureException);
                        return;
                    }

                    if (retriedSections.get() >= maxStageRetries) {
                        queryStateMachine.transitionToFailed(failureException);
                        return;
                    }

                    if (!RECOVERABLE_ERROR_CODES.contains(failureInfo.getErrorCode())) {
                        queryStateMachine.transitionToFailed(failureException);
                        return;
                    }

                    try {
                        if (sectionExecution.abort()) {
                            retriedSections.incrementAndGet();
                            nodeManager.refreshNodes();
                            startScheduling();
                        }
                    }
                    catch (Throwable t) {
                        if (failureException != t) {
                            failureException.addSuppressed(t);
                        }
                        queryStateMachine.transitionToFailed(failureException);
                    }
                }
                else if (state == FINISHED) {
                    // checks if there's any new sections available for execution and starts the scheduling if any
                    startScheduling();
                }
                else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (stageExecution.hasTasks()) {
                        queryStateMachine.transitionToRunning();
                    }
                }
            });
            stageExecution.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.of(getStageInfo())));
        }
    }

    private static boolean isRootFragment(PlanFragment fragment)
    {
        return fragment.getId().getId() == ROOT_FRAGMENT_ID;
    }

    @Override
    public long getUserMemoryReservation()
    {
        return getAllStagesExecutions().mapToLong(SqlStageExecution::getUserMemoryReservation).sum();
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return getAllStagesExecutions().mapToLong(SqlStageExecution::getTotalMemoryReservation).sum();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        long millis = getAllStagesExecutions()
                .map(SqlStageExecution::getTotalCpuTime)
                .mapToLong(Duration::toMillis)
                .sum();
        return new Duration(millis, MILLISECONDS);
    }

    @Override
    public DataSize getRawInputDataSize()
    {
        long rawInputDataSize = getAllStagesExecutions()
                .map(SqlStageExecution::getRawInputDataSize)
                .mapToLong(DataSize::toBytes)
                .sum();
        return DataSize.succinctBytes(rawInputDataSize);
    }

    @Override
    public DataSize getOutputDataSize()
    {
        return getStageInfo().getLatestAttemptExecutionInfo().getStats().getOutputDataSize();
    }

    @Override
    public BasicStageExecutionStats getBasicStageStats()
    {
        List<BasicStageExecutionStats> stageStats = getAllStagesExecutions()
                .map(SqlStageExecution::getBasicStageStats)
                .collect(toImmutableList());
        return aggregateBasicStageStats(stageStats);
    }

    private Stream<SqlStageExecution> getAllStagesExecutions()
    {
        return sectionExecutions.values().stream()
                .flatMap(Collection::stream)
                .flatMap(sectionExecution -> sectionExecution.getSectionStages().stream())
                .map(StageExecutionAndScheduler::getStageExecution);
    }

    @Override
    public StageInfo getStageInfo()
    {
        ListMultimap<StageId, SqlStageExecution> stageExecutions = getStageExecutions();
        return buildStageInfo(plan.get(), stageExecutions);
    }

    private StageInfo buildStageInfo(SubPlan subPlan, ListMultimap<StageId, SqlStageExecution> stageExecutions)
    {
        StageId stageId = getStageId(subPlan.getFragment().getId());
        List<SqlStageExecution> attempts = stageExecutions.get(stageId);

        StageExecutionInfo latestAttemptInfo = attempts.isEmpty() ?
                unscheduledExecutionInfo(stageId.getId(), queryStateMachine.isDone()) :
                getLast(attempts).getStageExecutionInfo();
        List<StageExecutionInfo> previousAttemptInfos = attempts.size() < 2 ?
                ImmutableList.of() :
                attempts.subList(0, attempts.size() - 1).stream()
                        .map(SqlStageExecution::getStageExecutionInfo)
                        .collect(toImmutableList());

        return new StageInfo(
                stageId,
                locationFactory.createStageLocation(stageId),
                Optional.of(subPlan.getFragment()),
                latestAttemptInfo,
                previousAttemptInfos,
                subPlan.getChildren().stream()
                        .map(plan -> buildStageInfo(plan, stageExecutions))
                        .collect(toImmutableList()),
                runtimeOptimizedStages.contains(stageId));
    }

    private ListMultimap<StageId, SqlStageExecution> getStageExecutions()
    {
        ImmutableListMultimap.Builder<StageId, SqlStageExecution> result = ImmutableListMultimap.builder();
        for (Collection<SectionExecution> sectionExecutionAttempts : sectionExecutions.values()) {
            for (SectionExecution sectionExecution : sectionExecutionAttempts) {
                for (StageExecutionAndScheduler stageExecution : sectionExecution.getSectionStages()) {
                    result.put(stageExecution.getStageExecution().getStageExecutionId().getStageId(), stageExecution.getStageExecution());
                }
            }
        }
        return result.build();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            getAllStagesExecutions()
                    .filter(execution -> execution.getStageExecutionId().getStageId().equals(stageId))
                    .forEach(SqlStageExecution::cancel);
        }
    }

    @Override
    public void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            checkState(queryStateMachine.isDone(), "query scheduler is expected to be aborted only if the query is finished: %s", queryStateMachine.getQueryState());
            getAllStagesExecutions().forEach(SqlStageExecution::abort);
        }
    }
}
