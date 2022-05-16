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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.scheduler.FixedSourcePartitionedScheduler.BucketedSplitPlacementPolicy;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.EmptySplit;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.split.SplitSource.SplitBatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.airlift.concurrent.MoreFutures.addSuccessCallback;
import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.MoreFutures.whenAnyComplete;
import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE;
import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.NO_ACTIVE_DRIVER_GROUP;
import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.SPLIT_QUEUES_FULL;
import static com.facebook.presto.execution.scheduler.ScheduleResult.BlockedReason.WAITING_FOR_SOURCE;
import static com.facebook.presto.spi.SplitContext.NON_CACHEABLE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Objects.requireNonNull;

/**
 * SOURCE_DISTRIBUTION的调度器
 * 主要是调度不分区、不分组的情况。默认会使用SOURCE_DISTRIBUTION，所以默认会使用该调度器进行调度。
 * 怎么理解呢？看其名字，SourcePartitioned，表示Source已经分区了，
 * 向ES这种，已经自动将数据分成多个部分了，每个split就是一个部分，所以不需要分区了。
 */
public class SourcePartitionedScheduler
        implements SourceScheduler
{
    private enum State
    {
        /**
         * No splits have been added to pendingSplits set.
         */
        INITIALIZED,

        /**
         * At least one split has been added to pendingSplits set.
         */
        SPLITS_ADDED,

        /**
         * All splits from underlying SplitSource have been discovered.
         * No more splits will be added to the pendingSplits set.
         */
        NO_MORE_SPLITS,

        /**
         * All splits have been provided to caller of this scheduler.
         * Cleanup operations are done (e.g., drainCompletelyScheduledLifespans has drained all driver groups).
         */
        FINISHED
    }

    private final SqlStageExecution stage; //
    private final SplitSource splitSource; // 切分资源
    private final SplitPlacementPolicy splitPlacementPolicy;
    private final int splitBatchSize;
    private final PlanNodeId partitionedNode;
    private final boolean groupedExecution;

    // TODO: Add LIFESPAN_ADDED into SourcePartitionedScheduler#State and remove this boolean
    private boolean lifespanAdded;

    private final Map<Lifespan, ScheduleGroup> scheduleGroups = new HashMap<>();
    private State state = State.INITIALIZED;

    private SettableFuture<?> whenFinishedOrNewLifespanAdded = SettableFuture.create();

    public PlanNodeId getPlanNodeId()
    {
        return partitionedNode;
    }

    private SourcePartitionedScheduler(
            SqlStageExecution stage,
            PlanNodeId partitionedNode,
            SplitSource splitSource,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize,
            boolean groupedExecution)
    {
        this.stage = requireNonNull(stage, "stage is null");
        this.partitionedNode = requireNonNull(partitionedNode, "partitionedNode is null");
        this.splitSource = requireNonNull(splitSource, "splitSource is null");
        this.splitPlacementPolicy = requireNonNull(splitPlacementPolicy, "splitPlacementPolicy is null");

        checkArgument(splitBatchSize > 0, "splitBatchSize must be at least one");
        this.splitBatchSize = splitBatchSize;
        this.groupedExecution = groupedExecution;
    }

    /**
     * 创建SourcePartitionedScheduler作为StageScheduler（阶段调度器）
     * Obtains an instance of {@code SourcePartitionedScheduler} suitable for use as a
     * stage scheduler.
     * <p>
     * This returns an ungrouped {@code SourcePartitionedScheduler} that requires
     * minimal management from the caller, which is ideal for use as a stage scheduler.
     */
    public static StageScheduler newSourcePartitionedSchedulerAsStageScheduler(
            SqlStageExecution stage,
            PlanNodeId partitionedNode,
            SplitSource splitSource,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize)
    {
        SourcePartitionedScheduler sourcePartitionedScheduler = new SourcePartitionedScheduler(stage, partitionedNode, splitSource, splitPlacementPolicy, splitBatchSize, false);
        // 不分组不分区
        sourcePartitionedScheduler.startLifespan(Lifespan.taskWide(), NOT_PARTITIONED);

        // 创建SOURCE_DISTRIBUTION的调度器，在调度方法中本质上使用了sourcePartitionedScheduler进行调度
        return new StageScheduler() {
            @Override
            public ScheduleResult schedule()
            {
                // TODO 在调度的时候本质上是调用了 SourcePartitionedScheduler 去调度。
                ScheduleResult scheduleResult = sourcePartitionedScheduler.schedule();
                sourcePartitionedScheduler.drainCompletelyScheduledLifespans();
                return scheduleResult;
            }

            @Override
            public void close()
            {
                sourcePartitionedScheduler.close();
            }
        };
    }

    /**
     *
     * Obtains a {@code SourceScheduler} suitable for use in FixedSourcePartitionedScheduler.
     * <p>
     * This returns a {@code SourceScheduler} that can be used for a pipeline
     * that is either ungrouped or grouped. However, the caller is responsible initializing
     * the driver groups in this scheduler accordingly.
     * <p>
     * Besides, the caller is required to poll {@link #drainCompletelyScheduledLifespans()}
     * in addition to {@link #schedule()} on the returned object. Otherwise, lifecycle
     * transitioning of the object will not work properly.
     */
    public static SourceScheduler newSourcePartitionedSchedulerAsSourceScheduler(
            SqlStageExecution stage,
            PlanNodeId partitionedNode,
            SplitSource splitSource,
            SplitPlacementPolicy splitPlacementPolicy,
            int splitBatchSize,
            boolean groupedExecution)
    {
        return new SourcePartitionedScheduler(stage, partitionedNode, splitSource, splitPlacementPolicy, splitBatchSize, groupedExecution);
    }

    /**
     * 开始生命周期
     * @param lifespan
     * @param partitionHandle ：很多情况可能未分区
     */
    @Override
    public synchronized void startLifespan(Lifespan lifespan, ConnectorPartitionHandle partitionHandle)
    {
        checkState(state == State.INITIALIZED || state == State.SPLITS_ADDED);
        lifespanAdded = true;
        scheduleGroups.put(lifespan, new ScheduleGroup(partitionHandle));
        whenFinishedOrNewLifespanAdded.set(null);
        whenFinishedOrNewLifespanAdded = SettableFuture.create();
    }

    /**
     * 倒回Lifespan
     * @param lifespan
     * @param partitionHandle
     */
    @Override
    public synchronized void rewindLifespan(Lifespan lifespan, ConnectorPartitionHandle partitionHandle)
    {
        checkState(state == State.INITIALIZED || state == State.SPLITS_ADDED, "Current state %s is not rewindable", state);
        checkState(lifespanAdded, "Cannot rewind lifespan without any lifespan added before");
        scheduleGroups.remove(lifespan);
        splitSource.rewind(partitionHandle);
    }

    /**
     * 调度source task
     * @return
     */
    @Override
    public synchronized ScheduleResult schedule()
    {
        dropListenersFromWhenFinishedOrNewLifespansAdded();

        // 记录所有已分配的Split的数量
        int overallSplitAssignmentCount = 0;
        // 全部的新任务
        ImmutableSet.Builder<RemoteTask> overallNewTasks = ImmutableSet.builder();
        // 获取split时阻塞的集合
        List<ListenableFuture<?>> overallBlockedFutures = new ArrayList<>();
        boolean anyBlockedOnPlacements = false;
        boolean anyBlockedOnNextSplitBatch = false;
        boolean anyNotBlocked = false;

        // 遍历调度组
        for (Entry<Lifespan, ScheduleGroup> entry : scheduleGroups.entrySet()) {
            // 生命周期
            Lifespan lifespan = entry.getKey();
            ScheduleGroup scheduleGroup = entry.getValue();

            // 如果没有分片了，确认是否为空
            if (scheduleGroup.state == ScheduleGroupState.NO_MORE_SPLITS || scheduleGroup.state == ScheduleGroupState.DONE) {
                verify(scheduleGroup.nextSplitBatchFuture == null);
            }
            // 如果没有待处理的spilit，则拉取
            else if (scheduleGroup.pendingSplits.isEmpty()) {
                // try to get the next batch
                // 尝试获取下一批次
                if (scheduleGroup.nextSplitBatchFuture == null) {
                    scheduleGroup.nextSplitBatchFuture = splitSource.getNextBatch(scheduleGroup.partitionHandle, lifespan, splitBatchSize);

                    // 添加回调
                    long start = System.nanoTime();
                    addSuccessCallback(scheduleGroup.nextSplitBatchFuture, () -> stage.recordGetSplitTime(start));
                }

                // 获取下一批split完成
                if (scheduleGroup.nextSplitBatchFuture.isDone()) {
                    SplitBatch nextSplits = getFutureValue(scheduleGroup.nextSplitBatchFuture);
                    scheduleGroup.nextSplitBatchFuture = null;
                    scheduleGroup.pendingSplits = new HashSet<>(nextSplits.getSplits());
                    // 如果是最后一批，设置成NO_MORE_SPLITS状态
                    if (nextSplits.isLastBatch()) {
                        if (scheduleGroup.state == ScheduleGroupState.INITIALIZED && scheduleGroup.pendingSplits.isEmpty()) {
                            // Add an empty split in case no splits have been produced for the source.
                            // For source operators, they never take input, but they may produce output.
                            // This is well handled by Presto execution engine.
                            // However, there are certain non-source operators that may produce output without any input,
                            // for example, 1) an AggregationOperator, 2) a HashAggregationOperator where one of the grouping sets is ().
                            // Scheduling an empty split kicks off necessary driver instantiation to make this work.
                            // 创建一个split
                            scheduleGroup.pendingSplits.add(new Split(
                                    splitSource.getConnectorId(),
                                    splitSource.getTransactionHandle(),
                                    new EmptySplit(splitSource.getConnectorId()),
                                    lifespan,
                                    NON_CACHEABLE));
                        }
                        // 最后一批次，设置状态为NO_MORE_SPLITS
                        scheduleGroup.state = ScheduleGroupState.NO_MORE_SPLITS;
                    }
                }
                else {
                    overallBlockedFutures.add(scheduleGroup.nextSplitBatchFuture);
                    anyBlockedOnNextSplitBatch = true;
                    continue;
                }
            }

            // Split放置结果
            Multimap<InternalNode, Split> splitAssignment = ImmutableMultimap.of();
            // 如果已经有准备好的split
            if (!scheduleGroup.pendingSplits.isEmpty()) {
                if (!scheduleGroup.placementFuture.isDone()) {
                    anyBlockedOnPlacements = true;
                    continue;
                }

                // 更新调度状态为SPLITS_ADDED
                if (scheduleGroup.state == ScheduleGroupState.INITIALIZED) {
                    scheduleGroup.state = ScheduleGroupState.SPLITS_ADDED;
                }
                if (state == State.INITIALIZED) {
                    state = State.SPLITS_ADDED;
                }

                // calculate placements for splits
                // TODO 计算split的位置，也就是将split放置到Worker节点上
                SplitPlacementResult splitPlacementResult = splitPlacementPolicy.computeAssignments(scheduleGroup.pendingSplits);
                splitAssignment = splitPlacementResult.getAssignments();

                // remove splits with successful placements
                // 从scheduleGroup.pendingSplits中移除已经成功分配节点的splits
                splitAssignment.values().forEach(scheduleGroup.pendingSplits::remove); // AbstractSet.removeAll performs terribly here.
                overallSplitAssignmentCount += splitAssignment.size();

                // if not completed placed, mark scheduleGroup as blocked on placement
                // 如果仍有split处于pening未被分配，则将该scheduleGroup标记为阻塞
                if (!scheduleGroup.pendingSplits.isEmpty()) {
                    scheduleGroup.placementFuture = splitPlacementResult.getBlocked();
                    overallBlockedFutures.add(scheduleGroup.placementFuture);
                    anyBlockedOnPlacements = true;
                }
            }

            // if no new splits will be assigned, update state and attach completion event
            // 如果没有新splits待分配，则更新scheduleGroup状态为DONE，并为每个节点添加完成事件
            // TODO 没有更多Splits，如果不为空，会对remote task进行通知
            Multimap<InternalNode, Lifespan> noMoreSplitsNotification = ImmutableMultimap.of();
            // 如果挂起的split为空
            if (scheduleGroup.pendingSplits.isEmpty() && scheduleGroup.state == ScheduleGroupState.NO_MORE_SPLITS) {
                // 调度组状态设置为DONE
                scheduleGroup.state = ScheduleGroupState.DONE;
                // 如果是分组，TaskWide是不分组
                if (!lifespan.isTaskWide()) {
                    InternalNode node = ((BucketedSplitPlacementPolicy) splitPlacementPolicy).getNodeForBucket(lifespan.getId());
                    noMoreSplitsNotification = ImmutableMultimap.of(node, lifespan);
                }
            }

            // assign the splits with successful placements
            // 将对应分配的分片与完成事件消息分发给节点，并将分发的task添加到总task列表中
            // TODO 这里是关键，在这里会将split调度到Worker
            overallNewTasks.addAll(assignSplits(splitAssignment, noMoreSplitsNotification));

            // Assert that "placement future is not done" implies "pendingSplits is not empty".
            // The other way around is not true. One obvious reason is (un)lucky timing, where the placement is unblocked between `computeAssignments` and this line.
            // However, there are other reasons that could lead to this.
            // Note that `computeAssignments` is quite broken:
            // 1. It always returns a completed future when there are no tasks, regardless of whether all nodes are blocked.
            // 2. The returned future will only be completed when a node with an assigned task becomes unblocked. Other nodes don't trigger future completion.
            // As a result, to avoid busy loops caused by 1, we check pendingSplits.isEmpty() instead of placementFuture.isDone() here.
            // 断言“放置未完成”意味着“pendingSplits不为空”。
            if (scheduleGroup.nextSplitBatchFuture == null && scheduleGroup.pendingSplits.isEmpty() && scheduleGroup.state != ScheduleGroupState.DONE) {
                anyNotBlocked = true;
            }
        }

        // * `splitSource.isFinished` invocation may fail after `splitSource.close` has been invoked.
        //   If state is NO_MORE_SPLITS/FINISHED, splitSource.isFinished has previously returned true, and splitSource is closed now.
        // * Even if `splitSource.isFinished()` return true, it is not necessarily safe to tear down the split source.
        //   * If anyBlockedOnNextSplitBatch is true, it means we have not checked out the recently completed nextSplitBatch futures,
        //     which may contain recently published splits. We must not ignore those.
        //   * If any scheduleGroup is still in DISCOVERING_SPLITS state, it means it hasn't realized that there will be no more splits.
        //     Next time it invokes getNextBatch, it will realize that. However, the invocation will fail we tear down splitSource now.
        //
        // Since grouped execution is going to support failure recovery, and scheduled splits might have to be rescheduled during retry,
        // we can no longer claim schedule is complete after all splits are scheduled.
        // Splits schedule can only be considered as finished when all lifespan executions are done
        // (by calling `notifyAllLifespansFinishedExecution`)
        // 更新状态
        if ((state == State.NO_MORE_SPLITS || state == State.FINISHED) || (!groupedExecution && lifespanAdded && scheduleGroups.isEmpty() && splitSource.isFinished())) {
            switch (state) {
                case INITIALIZED:
                    // We have not scheduled a single split so far.
                    // But this shouldn't be possible. See usage of EmptySplit in this method.
                    throw new IllegalStateException("At least 1 split should have been scheduled for this plan node");
                case SPLITS_ADDED:
                    state = State.NO_MORE_SPLITS;
                    splitSource.close();
                    // fall through
                case NO_MORE_SPLITS:
                    state = State.FINISHED;
                    whenFinishedOrNewLifespanAdded.set(null);
                    // fall through
                case FINISHED:
                    return ScheduleResult.nonBlocked(
                            true,
                            overallNewTasks.build(),
                            overallSplitAssignmentCount);
                default:
                    throw new IllegalStateException("Unknown state");
            }
        }

        // 没有任何阻塞，则返回未阻塞的调度结果。
        if (anyNotBlocked) {
            return ScheduleResult.nonBlocked(false, overallNewTasks.build(), overallSplitAssignmentCount);
        }

        // 以下就是被阻塞的情况了。
        if (anyBlockedOnPlacements) {
            // In a broadcast join, output buffers of the tasks in build source stage have to
            // hold onto all data produced before probe side task scheduling finishes,
            // even if the data is acknowledged by all known consumers. This is because
            // new consumers may be added until the probe side task scheduling finishes.
            //
            // As a result, the following line is necessary to prevent deadlock
            // due to neither build nor probe can make any progress.
            // The build side blocks due to a full output buffer.
            // In the meantime the probe side split cannot be consumed since
            // builder side hash table construction has not finished.
            // 在广播联接中，构建源阶段中任务的输出缓冲区，必须保留探测器端任务调度完成前生成的所有数据，即使数据已被所有已知消费者确认。
            // 这是因为在探测端任务调度完成之前，可能会添加新的使用者。、

            // 因此，以下行是防止死锁所必需的，由于构建和探测都无法取得任何进展。由于构建和探测都无法取得任何进展。由于输出缓冲区已满，生成端会阻塞。
            // /同时，由于生成器端哈希表构造尚未完成。
            // TODO: When SourcePartitionedScheduler is used as a SourceScheduler, it shouldn't need to worry about
            //  task scheduling and creation -- these are done by the StageScheduler.
            overallNewTasks.addAll(finalizeTaskCreationIfNecessary());
        }

        // 设置阻塞原因
        ScheduleResult.BlockedReason blockedReason;
        if (anyBlockedOnNextSplitBatch) {
            blockedReason = anyBlockedOnPlacements ? MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE : WAITING_FOR_SOURCE;
        }
        else {
            blockedReason = anyBlockedOnPlacements ? SPLIT_QUEUES_FULL : NO_ACTIVE_DRIVER_GROUP;
        }

        overallBlockedFutures.add(whenFinishedOrNewLifespanAdded);
        // 返回阻塞类型的调度结果
        return ScheduleResult.blocked(
                false,
                overallNewTasks.build(),
                nonCancellationPropagating(whenAnyComplete(overallBlockedFutures)),
                blockedReason,
                overallSplitAssignmentCount);
    }

    private synchronized void dropListenersFromWhenFinishedOrNewLifespansAdded()
    {
        // whenFinishedOrNewLifespanAdded may remain in a not-done state for an extended period of time.
        // As a result, over time, it can retain a huge number of listener objects.

        // Whenever schedule is called, holding onto the previous listener is not useful anymore.
        // Therefore, we drop those listeners here by recreating the future.

        // Note: The following implementation is thread-safe because whenFinishedOrNewLifespanAdded can only be completed
        // while holding the monitor of this.

        if (whenFinishedOrNewLifespanAdded.isDone()) {
            return;
        }

        whenFinishedOrNewLifespanAdded.cancel(true);
        whenFinishedOrNewLifespanAdded = SettableFuture.create();
    }

    @Override
    public void close()
    {
        splitSource.close();
    }

    /**
     * 完成生命周期
     * @return
     */
    @Override
    public synchronized List<Lifespan> drainCompletelyScheduledLifespans()
    {
        if (scheduleGroups.isEmpty()) {
            // Invoking splitSource.isFinished would fail if it was already closed, which is possible if scheduleGroups is empty.
            return ImmutableList.of();
        }

        ImmutableList.Builder<Lifespan> result = ImmutableList.builder();
        Iterator<Entry<Lifespan, ScheduleGroup>> entryIterator = scheduleGroups.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Entry<Lifespan, ScheduleGroup> entry = entryIterator.next();
            if (entry.getValue().state == ScheduleGroupState.DONE) {
                result.add(entry.getKey());
                entryIterator.remove();
            }
        }

        if (scheduleGroups.isEmpty() && splitSource.isFinished()) {
            // Wake up blocked caller so that it will invoke schedule() right away.
            // Once schedule is invoked, state will be transitioned to FINISHED.
            whenFinishedOrNewLifespanAdded.set(null);
            whenFinishedOrNewLifespanAdded = SettableFuture.create();
        }

        return result.build();
    }

    @Override
    public synchronized void notifyAllLifespansFinishedExecution()
    {
        checkState(groupedExecution);
        state = State.FINISHED;
        splitSource.close();
        whenFinishedOrNewLifespanAdded.set(null);
    }

    /**
     * 分配splits
     * @param splitAssignment
     * @param noMoreSplitsNotification
     * @return
     */
    private Set<RemoteTask> assignSplits(Multimap<InternalNode, Split> splitAssignment, Multimap<InternalNode, Lifespan> noMoreSplitsNotification)
    {
        // 新任务
        ImmutableSet.Builder<RemoteTask> newTasks = ImmutableSet.builder();

        ImmutableSet<InternalNode> nodes = ImmutableSet.<InternalNode>builder()
                .addAll(splitAssignment.keySet())
                .addAll(noMoreSplitsNotification.keySet())
                .build();
        // 遍历所有集群节点
        for (InternalNode node : nodes) {
            // worker 节点
            ImmutableMultimap<PlanNodeId, Split> splits = ImmutableMultimap.<PlanNodeId, Split>builder()
                    .putAll(partitionedNode, splitAssignment.get(node))
                    .build();

            // 没有split集合
            ImmutableMultimap.Builder<PlanNodeId, Lifespan> noMoreSplits = ImmutableMultimap.builder();
            if (noMoreSplitsNotification.containsKey(node)) {
                noMoreSplits.putAll(partitionedNode, noMoreSplitsNotification.get(node));
            }

            // TODO 调度split
            newTasks.addAll(stage.scheduleSplits(
                    node,
                    splits,
                    noMoreSplits.build()));
        }
        return newTasks.build();
    }

    private Set<RemoteTask> finalizeTaskCreationIfNecessary()
    {
        // only lock down tasks if there is a sub stage that could block waiting for this stage to create all tasks
        if (stage.getFragment().isLeaf()) {
            return ImmutableSet.of();
        }

        splitPlacementPolicy.lockDownNodes();

        Set<InternalNode> scheduledNodes = stage.getScheduledNodes();
        Set<RemoteTask> newTasks = splitPlacementPolicy.getActiveNodes().stream()
                .filter(node -> !scheduledNodes.contains(node))
                .flatMap(node -> stage.scheduleSplits(node, ImmutableMultimap.of(), ImmutableMultimap.of()).stream())
                .collect(toImmutableSet());

        // notify listeners that we have scheduled all tasks so they can set no more buffers or exchange splits
        stage.transitionToFinishedTaskScheduling();

        return newTasks;
    }

    /**
     * 调度组
     */
    private static class ScheduleGroup
    {
        // 分区句柄
        public final ConnectorPartitionHandle partitionHandle;
        // 下一批次split
        public ListenableFuture<SplitBatch> nextSplitBatchFuture;
        //
        public ListenableFuture<?> placementFuture = Futures.immediateFuture(null);
        // 挂起的split，待调度的split
        public Set<Split> pendingSplits = new HashSet<>();
        // 调度分组初始状态
        public ScheduleGroupState state = ScheduleGroupState.INITIALIZED;

        public ScheduleGroup(ConnectorPartitionHandle partitionHandle)
        {
            this.partitionHandle = requireNonNull(partitionHandle, "partitionHandle is null");
        }
    }

    /**
     * 调度组状态
     */
    private enum ScheduleGroupState
    {
        /**
         * No splits have been added to pendingSplits set.
         */
        INITIALIZED,

        /**
         * At least one split has been added to pendingSplits set.
         */
        SPLITS_ADDED,

        /**
         * All splits from underlying SplitSource has been discovered.
         * No more splits will be added to the pendingSplits set.
         */
        NO_MORE_SPLITS,

        /**
         * All splits has been provided to caller of this scheduler.
         * Cleanup operations (e.g. inform caller of noMoreSplits) are done.
         */
        DONE
    }
}
