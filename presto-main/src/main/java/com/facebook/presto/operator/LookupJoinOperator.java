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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.JoinProbe.JoinProbeFactory;
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.operator.LookupSourceProvider.LookupSourceLease;
import com.facebook.presto.operator.PartitionedConsumption.Partition;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spiller.PartitioningSpiller;
import com.facebook.presto.spiller.PartitioningSpiller.PartitioningSpillResult;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.MoreFutures.addSuccessCallback;
import static com.facebook.airlift.concurrent.MoreFutures.getDone;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.facebook.presto.operator.SpillingUtils.checkSpillSucceeded;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * Hash Join的probe探测端
 * 通过lookup这个单词我们知道是查找，去哪里查找啊，去构建表中查找，所以LookupJoinOperator完成的是探测端的操作
 */
public class LookupJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> probeTypes;
    private final JoinProbeFactory joinProbeFactory;
    private final Runnable afterClose;
    private final OptionalInt lookupJoinsCount;
    private final HashGenerator hashGenerator;
    private final LookupSourceFactory lookupSourceFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private final JoinStatisticsCounter statisticsCounter;

    private final LookupJoinPageBuilder pageBuilder;

    private final boolean probeOnOuterSide;

    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;
    private LookupSourceProvider lookupSourceProvider;
    private JoinProbe probe;

    private Page outputPage;

    private Optional<PartitioningSpiller> spiller = Optional.empty();
    private Optional<LocalPartitionGenerator> partitionGenerator = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED; // 溢出过程
    private long inputPageSpillEpoch;
    private boolean closed;
    private boolean finishing; // 是否处理完成
    private boolean unspilling;
    private boolean finished;
    private long joinPosition = -1;
    private int joinSourcePositions;

    private boolean currentProbePositionProducedRow;

    private final Map<Integer, SavedRow> savedRows = new HashMap<>();
    @Nullable
    private ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> partitionedConsumption;
    @Nullable
    private Iterator<Partition<Supplier<LookupSource>>> lookupPartitions;
    private Optional<Partition<Supplier<LookupSource>>> currentPartition = Optional.empty();
    private Optional<ListenableFuture<Supplier<LookupSource>>> unspilledLookupSource = Optional.empty();
    private Iterator<Page> unspilledInputPages = emptyIterator();

    public LookupJoinOperator(
            OperatorContext operatorContext,
            List<Type> probeTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable afterClose,
            OptionalInt lookupJoinsCount,
            HashGenerator hashGenerator,
            PartitioningSpillerFactory partitioningSpillerFactory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));

        requireNonNull(joinType, "joinType is null");
        // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
        probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;

        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        // 在创建LookupJoinOperator算子的
        // PartitionedLookupSourceFactory#createLookupSourceProvider
        // 所以在本类中获取到的lookupSourceProvider是SpillAwareLookupSourceProvider
        this.lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();

        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        operatorContext.setInfoSupplier(this.statisticsCounter);

        this.pageBuilder = new LookupJoinPageBuilder(buildOutputTypes);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (finishing) {
            return;
        }

        if (!spillInProgress.isDone()) {
            return;
        }

        checkSpillSucceeded(spillInProgress);
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = this.finished && probe == null && pageBuilder.isEmpty() && outputPage == null;

        // if finished drop references so memory is freed early
        if (finished) {
            close();
        }
        return finished;
    }

    /**
     * 是否阻塞
     * @return
     */
    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!spillInProgress.isDone()) {
            // Input spilling can happen only after lookupSourceProviderFuture was done.
            return spillInProgress;
        }
        if (unspilledLookupSource.isPresent()) {
            // Unspilling can happen only after lookupSourceProviderFuture was done.
            return unspilledLookupSource.get();
        }

        if (finishing) {
            return NOT_BLOCKED;
        }

        // 被阻塞的原因就是source端，即hash表没有构建号，如果没有构建则返回空，如果构造好了，则返回一个lookupSourceProviderFuture对象
        return lookupSourceProviderFuture;
    }

    @Override
    public boolean needsInput()
    {
        // 这里会尝试判断lookupSourceProviderFuture是否已经完成
        return !finishing
                && lookupSourceProviderFuture.isDone()
                && spillInProgress.isDone()
                && probe == null
                && outputPage == null;
    }

    /**
     * 添加输入
     * @param page
     */
    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(probe == null, "Current page has not been completely processed yet");

        // 当添加的时候，一定是构建测已经构建好了，尝试拉取lookupSourceProvider
        checkState(tryFetchLookupSourceProvider(), "Not ready to handle input yet");

        // 获取一个租约，该租约和一个分区有关联
        SpillInfoSnapshot spillInfoSnapshot = lookupSourceProvider.withLease(SpillInfoSnapshot::from);
        // 添加page
        addInput(page, spillInfoSnapshot);
    }

    private void addInput(Page page, SpillInfoSnapshot spillInfoSnapshot)
    {
        requireNonNull(spillInfoSnapshot, "spillInfoSnapshot is null");

        // 判断是否已经溢出spill
        if (spillInfoSnapshot.hasSpilled()) {
            // 记录溢出的位置，并将溢出的部分写入磁盘
            page = spillAndMaskSpilledPositions(page, spillInfoSnapshot.getSpillMask());
            if (page.getPositionCount() == 0) {
                return;
            }
        }

        // create probe
        inputPageSpillEpoch = spillInfoSnapshot.getSpillEpoch();
        // 创建探测测
        probe = joinProbeFactory.createJoinProbe(page);

        // initialize to invalid join position to force output code to advance the cursors
        // 初始化为无效的联接位置，以强制输出代码前进光标
        joinPosition = -1;
    }

    /**
     * 尝试获取LookupSourceProvider
     * @return
     */
    private boolean tryFetchLookupSourceProvider()
    {
        if (lookupSourceProvider == null) {
            if (!lookupSourceProviderFuture.isDone()) {
                return false;
            }
            // 如果构建测已经完成
            lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderFuture));
            // 更新
            statisticsCounter.updateLookupSourcePositions(lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().getJoinPositionCount()));
        }
        return true;
    }

    private Page spillAndMaskSpilledPositions(Page page, IntPredicate spillMask)
    {
        checkState(spillInProgress.isDone(), "Previous spill still in progress");
        checkSpillSucceeded(spillInProgress);

        if (!spiller.isPresent()) {
            spiller = Optional.of(partitioningSpillerFactory.create(
                    probeTypes,
                    getPartitionGenerator(),
                    operatorContext.getSpillContext().newLocalSpillContext(),
                    operatorContext.newAggregateSystemMemoryContext()));
        }

        PartitioningSpillResult result = spiller.get().partitionAndSpill(page, spillMask);
        spillInProgress = result.getSpillingFuture();
        return result.getRetained();
    }

    public LocalPartitionGenerator getPartitionGenerator()
    {
        if (!partitionGenerator.isPresent()) {
            partitionGenerator = Optional.of(new LocalPartitionGenerator(hashGenerator, lookupSourceFactory.partitions()));
        }
        return partitionGenerator.get();
    }

    /**
     * 获取输出信息
     * @return
     */
    @Override
    public Page getOutput()
    {
        // TODO introduce explicit state (enum), like in HBO

        if (!spillInProgress.isDone()) {
            /*
             * We cannot produce output when there is some previous input spilling. This is because getOutput() may result in additional portion of input being spilled
             * (when spilling state has changed in partitioned lookup source since last time) and spiller does not allow multiple concurrent spills.
             */
            return null;
        }

        checkSpillSucceeded(spillInProgress);

        if (probe == null && pageBuilder.isEmpty() && !finishing) {
            return null;
        }

        // 尝试判断是否已经构建完成
        // 如果没有完成，则返回null
        if (!tryFetchLookupSourceProvider()) {
            if (!finishing) {
                return null;
            }

            verify(finishing);
            // We are no longer interested in the build side (the lookupSourceProviderFuture's value).
            addSuccessCallback(lookupSourceProviderFuture, LookupSourceProvider::close);
            lookupSourceProvider = new StaticLookupSourceProvider(new EmptyLookupSource());
        }

        // 完成探测侧算子
        if (probe == null && finishing && !unspilling) {
            /*
             * We do not have input probe and we won't have any, as we're finishing.
             * Let LookupSourceFactory know LookupSources can be disposed as far as we're concerned.
             */
            verify(partitionedConsumption == null, "partitioned consumption already started");
            partitionedConsumption = lookupSourceFactory.finishProbeOperator(lookupJoinsCount);
            unspilling = true;
        }

        if (probe == null && unspilling && !finished) {
            /*
             * If no current partition or it was exhausted, unspill next one.
             * Add input there when it needs one, produce output. Be Happy.
             */
            tryUnspillNext();
        }

        //
        if (probe != null) {
            // 处理探测侧
            processProbe();
        }

        // 如果输出page不为空，则返回当前的输出page，并置空
        if (outputPage != null) {
            verify(pageBuilder.isEmpty());
            Page output = outputPage;
            outputPage = null;
            return output;
        }

        // It is impossible to have probe == null && !pageBuilder.isEmpty(),
        // because we will flush a page whenever we reach the probe end
        verify(probe != null || pageBuilder.isEmpty());
        return null;
    }

    private void tryUnspillNext()
    {
        verify(probe == null);

        if (!partitionedConsumption.isDone()) {
            return;
        }

        if (lookupPartitions == null) {
            lookupPartitions = getDone(partitionedConsumption).beginConsumption();
        }

        if (unspilledInputPages.hasNext()) {
            addInput(unspilledInputPages.next());
            return;
        }

        if (unspilledLookupSource.isPresent()) {
            if (!unspilledLookupSource.get().isDone()) {
                // Not unspilled yet
                return;
            }
            LookupSource lookupSource = getDone(unspilledLookupSource.get()).get();
            unspilledLookupSource = Optional.empty();

            // Close previous lookupSourceProvider (either supplied initially or for the previous partition)
            lookupSourceProvider.close();
            lookupSourceProvider = new StaticLookupSourceProvider(lookupSource);
            // If the partition was spilled during processing, its position count will be considered twice.
            statisticsCounter.updateLookupSourcePositions(lookupSource.getJoinPositionCount());

            int partition = currentPartition.get().number();
            unspilledInputPages = spiller.map(spiller -> spiller.getSpilledPages(partition))
                    .orElse(emptyIterator());

            Optional.ofNullable(savedRows.remove(partition)).ifPresent(savedRow -> {
                restoreProbe(
                        savedRow.row,
                        savedRow.joinPositionWithinPartition,
                        savedRow.currentProbePositionProducedRow,
                        savedRow.joinSourcePositions,
                        SpillInfoSnapshot.noSpill());
            });

            return;
        }

        if (lookupPartitions.hasNext()) {
            currentPartition.ifPresent(Partition::release);
            currentPartition = Optional.of(lookupPartitions.next());
            unspilledLookupSource = Optional.of(currentPartition.get().load());

            return;
        }

        currentPartition.ifPresent(Partition::release);
        if (lookupSourceProvider != null) {
            // There are no more partitions to process, so clean up everything
            lookupSourceProvider.close();
            lookupSourceProvider = null;
        }
        spiller.ifPresent(PartitioningSpiller::verifyAllPartitionsRead);
        finished = true;
    }

    /**
     * 处理探测侧
     */
    private void processProbe()
    {
        verify(probe != null);

        //
        Optional<SpillInfoSnapshot> spillInfoSnapshotIfSpillChanged = lookupSourceProvider.withLease(lookupSourceLease -> {
            //
            if (lookupSourceLease.spillEpoch() == inputPageSpillEpoch) {
                // Spill state didn't change, so process as usual.
                processProbe(lookupSourceLease.getLookupSource());
                return Optional.empty();
            }

            return Optional.of(SpillInfoSnapshot.from(lookupSourceLease));
        });

        if (!spillInfoSnapshotIfSpillChanged.isPresent()) {
            return;
        }
        SpillInfoSnapshot spillInfoSnapshot = spillInfoSnapshotIfSpillChanged.get();
        long joinPositionWithinPartition;
        if (joinPosition >= 0) {
            joinPositionWithinPartition = lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().joinPositionWithinPartition(joinPosition));
        }
        else {
            joinPositionWithinPartition = -1;
        }

        /*
         * Spill state changed. All probe rows that were not processed yet should be treated as regular input (and be partially spilled).
         * If current row maps to the now-spilled partition, it needs to be saved for later. If it maps to a partition still in memory, it
         * should be added together with not-yet-processed rows. In either case we need to resume processing the row starting at its
         * current position in the lookup source.
         */
        verify(spillInfoSnapshot.hasSpilled());
        verify(spillInfoSnapshot.getSpillEpoch() > inputPageSpillEpoch);

        Page currentPage = probe.getPage();
        int currentPosition = probe.getPosition();
        long currentJoinPosition = this.joinPosition;
        boolean currentProbePositionProducedRow = this.currentProbePositionProducedRow;

        clearProbe();

        if (currentPosition < 0) {
            // Processing of the page hasn't been started yet.
            addInput(currentPage, spillInfoSnapshot);
        }
        else {
            int currentRowPartition = getPartitionGenerator().getPartition(currentPage, currentPosition);
            boolean currentRowSpilled = spillInfoSnapshot.getSpillMask().test(currentRowPartition);

            if (currentRowSpilled) {
                savedRows.merge(
                        currentRowPartition,
                        new SavedRow(currentPage, currentPosition, joinPositionWithinPartition, currentProbePositionProducedRow, joinSourcePositions),
                        (oldValue, newValue) -> {
                            throw new IllegalStateException(format("Partition %s is already spilled", currentRowPartition));
                        });
                joinSourcePositions = 0;
                Page unprocessed = pageTail(currentPage, currentPosition + 1);
                addInput(unprocessed, spillInfoSnapshot);
            }
            else {
                Page remaining = pageTail(currentPage, currentPosition);
                restoreProbe(remaining, currentJoinPosition, currentProbePositionProducedRow, joinSourcePositions, spillInfoSnapshot);
            }
        }
    }

    /**
     * 处理探测
     * @param lookupSource
     */
    private void processProbe(LookupSource lookupSource)
    {
        verify(probe != null);

        // 驱动让行信号
        DriverYieldSignal yieldSignal = operatorContext.getDriverContext().getYieldSignal();
        // 如果没有让行，则处理当前业务
        // 轮询，直到让行，或者，构建完成了一个page。
        while (!yieldSignal.isSet()) {
            if (probe.getPosition() >= 0) {
                // 如果让行 或者 成功构建了一个page算子
                // getOutput方法会将构建好的page，交给下一个算子
                if (!joinCurrentPosition(lookupSource, yieldSignal)) {
                    break;
                }
                if (!currentProbePositionProducedRow) {
                    currentProbePositionProducedRow = true;
                    // outer join 的情况
                    if (!outerJoinCurrentPosition()) {
                        break;
                    }
                }
            }
            currentProbePositionProducedRow = false;
            // 前进到下一个位置
            if (!advanceProbePosition(lookupSource)) {
                break;
            }
            // 记录recordProbe的信息
            statisticsCounter.recordProbe(joinSourcePositions);
            joinSourcePositions = 0;
        }
    }

    private void restoreProbe(Page probePage, long joinPosition, boolean currentProbePositionProducedRow, int joinSourcePositions, SpillInfoSnapshot spillInfoSnapshot)
    {
        verify(probe == null);

        addInput(probePage, spillInfoSnapshot);
        verify(probe.advanceNextPosition());
        this.joinPosition = joinPosition;
        this.currentProbePositionProducedRow = currentProbePositionProducedRow;
        this.joinSourcePositions = joinSourcePositions;
    }

    private Page pageTail(Page currentPage, int startAtPosition)
    {
        verify(currentPage.getPositionCount() - startAtPosition >= 0);
        return currentPage.getRegion(startAtPosition, currentPage.getPositionCount() - startAtPosition);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        probe = null;

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterClose::run);

            closer.register(pageBuilder::reset);
            closer.register(() -> Optional.ofNullable(lookupSourceProvider).ifPresent(LookupSourceProvider::close));
            spiller.ifPresent(closer::register);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 生成与当前探测位置的联接条件匹配的行。如果以前调用过此方法对于当前探测位置，再次调用此命令将生成上一次中未生成的行调用。
     *
     * Produce rows matching join condition for the current probe position. If this method was called previously
     * for the current probe position, calling this again will produce rows that wasn't been produced in previous
     * invocations.
     *
     * @return true if all eligible rows have been produced; false otherwise
     */
    private boolean joinCurrentPosition(LookupSource lookupSource, DriverYieldSignal yieldSignal)
    {
        // while we have a position on lookup side to join against...
        while (joinPosition >= 0) {
            // 如果probe的位置
            if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                currentProbePositionProducedRow = true;

                // 拼接一行
                pageBuilder.appendRow(probe, lookupSource, joinPosition);
                //
                joinSourcePositions++;
            }

            // get next position on lookup side for this probe row
            // 获取此探测行在查找侧的下一个位置
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

            // 如果让行，或者数据量可以构建一个page了，则返回false
            if (yieldSignal.isSet() || tryBuildPage()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return whether there are more positions on probe side
     */
    private boolean advanceProbePosition(LookupSource lookupSource)
    {
        if (!probe.advanceNextPosition()) {
            clearProbe();
            return false;
        }

        // update join position
        joinPosition = probe.getCurrentJoinPosition(lookupSource);
        return true;
    }

    /**
     * Produce a row for the current probe position, if it doesn't match any row on lookup side and this is an outer join.
     *
     * @return whether pageBuilder became full
     */
    private boolean outerJoinCurrentPosition()
    {
        if (probeOnOuterSide && joinPosition < 0) {
            pageBuilder.appendNullForBuild(probe);
            if (tryBuildPage()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 溢出信息快照
     */
    // This class must be public because LookupJoinOperator is isolated.
    public static class SpillInfoSnapshot
    {
        private final boolean hasSpilled;
        private final long spillEpoch; // 溢出时期
        private final IntPredicate spillMask;

        public SpillInfoSnapshot(boolean hasSpilled, long spillEpoch, IntPredicate spillMask)
        {
            this.hasSpilled = hasSpilled;
            this.spillEpoch = spillEpoch;
            this.spillMask = requireNonNull(spillMask, "spillMask is null");
        }

        public static SpillInfoSnapshot from(LookupSourceLease lookupSourceLease)
        {
            return new SpillInfoSnapshot(
                    lookupSourceLease.hasSpilled(),
                    lookupSourceLease.spillEpoch(),
                    lookupSourceLease.getSpillMask());
        }

        public static SpillInfoSnapshot noSpill()
        {
            return new SpillInfoSnapshot(false, 0, i -> false);
        }

        public boolean hasSpilled()
        {
            return hasSpilled;
        }

        public long getSpillEpoch()
        {
            return spillEpoch;
        }

        public IntPredicate getSpillMask()
        {
            return spillMask;
        }
    }

    // This class must be public because LookupJoinOperator is isolated.
    public static class SavedRow
    {
        /**
         * A page with exactly one {@link Page#getPositionCount}, representing saved row.
         */
        public final Page row;

        /**
         * A snapshot of {@link LookupJoinOperator#joinPosition} "de-partitioned", i.e. {@link LookupJoinOperator#joinPosition} is a join position
         * with respect to (potentially) partitioned lookup source, while this value is a join position with respect to containing partition.
         */
        public final long joinPositionWithinPartition;

        /**
         * A snapshot of {@link LookupJoinOperator#currentProbePositionProducedRow}
         */
        public final boolean currentProbePositionProducedRow;

        /**
         * A snapshot of {@link LookupJoinOperator#joinSourcePositions}
         */
        public final int joinSourcePositions;

        public SavedRow(Page page, int position, long joinPositionWithinPartition, boolean currentProbePositionProducedRow, int joinSourcePositions)
        {
            this.row = page.getSingleValuePage(position);

            this.joinPositionWithinPartition = joinPositionWithinPartition;
            this.currentProbePositionProducedRow = currentProbePositionProducedRow;
            this.joinSourcePositions = joinSourcePositions;
        }
    }

    /**
     * 尝试构建一个page
     * @return
     */
    private boolean tryBuildPage()
    {
        // 如果数据已经达到一个page的规模，则满了，表示可以构建一个page了。否则返回false
        if (pageBuilder.isFull()) {
            // 构建page
            buildPage();
            return true;
        }
        return false;
    }

    /**
     * 构建一个page，将其设置给outputPage
     */
    private void buildPage()
    {
        verify(outputPage == null);
        verify(probe != null);

        if (pageBuilder.isEmpty()) {
            return;
        }

        outputPage = pageBuilder.build(probe);
        pageBuilder.reset();
    }

    private void clearProbe()
    {
        // Before updating the probe flush the current page
        buildPage();
        probe = null;
    }
}
