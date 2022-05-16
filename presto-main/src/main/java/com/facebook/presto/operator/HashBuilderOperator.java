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
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Queue;

import static com.facebook.airlift.concurrent.MoreFutures.getDone;
import static com.facebook.presto.operator.SpillingUtils.checkSpillSucceeded;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Hash表的构建类
 * 1） addInput()时将Page累积在内存中；
 * 2） finish()时，则创建Hash Table；
 * 3） 不再阻塞LookupJoinOperator，即LookupJoinOperator可以开始处理。
 */
@ThreadSafe
public class HashBuilderOperator
        implements Operator
{
    public static class HashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;
        private final List<Integer> outputChannels;
        private final List<Integer> hashChannels;
        private final OptionalInt preComputedHashChannel;
        private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
        private final Optional<Integer> sortChannel;
        private final List<JoinFilterFunctionFactory> searchFunctionFactories;
        private final PagesIndex.Factory pagesIndexFactory;

        private final int expectedPositions;
        private final boolean spillEnabled;
        private final SingleStreamSpillerFactory singleStreamSpillerFactory;

        private final Map<Lifespan, Integer> partitionIndexManager = new HashMap<>();

        private boolean closed;
        private boolean enforceBroadcastMemoryLimit;

        public HashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
                List<Integer> outputChannels,
                List<Integer> hashChannels,
                OptionalInt preComputedHashChannel,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                Optional<Integer> sortChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                SingleStreamSpillerFactory singleStreamSpillerFactory,
                boolean enforceBroadcastMemoryLimit)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(sortChannel, "sortChannel can not be null");
            requireNonNull(searchFunctionFactories, "searchFunctionFactories is null");
            checkArgument(sortChannel.isPresent() != searchFunctionFactories.isEmpty(), "both or none sortChannel and searchFunctionFactories must be set");
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.sortChannel = sortChannel;
            this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");

            this.expectedPositions = expectedPositions;
            this.enforceBroadcastMemoryLimit = enforceBroadcastMemoryLimit;
        }

        /**
         * 创建HashBuilderOperator
         * @param driverContext
         * @return
         */
        @Override
        public HashBuilderOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            // 添加算子上下文
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderOperator.class.getSimpleName());
            // 分区查询工厂
            PartitionedLookupSourceFactory lookupSourceFactory = this.lookupSourceFactoryManager.getJoinBridge(driverContext.getLifespan());
            // 分区下标
            int partitionIndex = getAndIncrementPartitionIndex(driverContext.getLifespan());
            verify(partitionIndex < lookupSourceFactory.partitions());
            // 创建HashBuilderOperator
            return new HashBuilderOperator(
                    operatorContext,
                    lookupSourceFactory,
                    partitionIndex,
                    outputChannels,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    singleStreamSpillerFactory,
                    enforceBroadcastMemoryLimit);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
        }

        private int getAndIncrementPartitionIndex(Lifespan lifespan)
        {
            return partitionIndexManager.compute(lifespan, (k, v) -> v == null ? 1 : v + 1) - 1;
        }
    }

    @VisibleForTesting
    public enum State
    {
        /**
         * Operator accepts input
         */
        CONSUMING_INPUT,

        /**
         * Memory revoking occurred during {@link #CONSUMING_INPUT}. Operator accepts input and spills it
         */
        SPILLING_INPUT,

        /**
         * LookupSource已经构建并传递，没有发生任何泄漏
         * LookupSource has been built and passed on without any spill occurring
         */
        LOOKUP_SOURCE_BUILT,

        /**
         * Input has been finished and spilled
         */
        INPUT_SPILLED,

        /**
         * Spilled input is being unspilled
         */
        INPUT_UNSPILLING,

        /**
         * Spilled input has been unspilled, LookupSource built from it
         */
        INPUT_UNSPILLED_AND_BUILT,

        /**
         * No longer needed
         */
        CLOSED
    }

    private static final double INDEX_COMPACTION_ON_REVOCATION_TARGET = 0.8;

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final PartitionedLookupSourceFactory lookupSourceFactory;
    private final ListenableFuture<?> lookupSourceFactoryDestroyed;
    private final int partitionIndex;

    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final OptionalInt preComputedHashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final Optional<Integer> sortChannel;
    private final List<JoinFilterFunctionFactory> searchFunctionFactories;

    private final PagesIndex index; // pages索引

    private final boolean spillEnabled;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;

    private final HashCollisionsCounter hashCollisionsCounter;

    private State state = State.CONSUMING_INPUT;
    private Optional<ListenableFuture<?>> lookupSourceNotNeeded = Optional.empty();
    private final SpilledLookupSourceHandle spilledLookupSourceHandle = new SpilledLookupSourceHandle();
    private Optional<SingleStreamSpiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private Optional<ListenableFuture<List<Page>>> unspillInProgress = Optional.empty();
    @Nullable
    private LookupSourceSupplier lookupSourceSupplier;
    private OptionalLong lookupSourceChecksum = OptionalLong.empty();

    private Optional<Runnable> finishMemoryRevoke = Optional.empty();

    private final boolean enforceBroadcastMemoryLimit;

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public HashBuilderOperator(
            OperatorContext operatorContext,
            PartitionedLookupSourceFactory lookupSourceFactory,
            int partitionIndex,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            OptionalInt preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            boolean enforceBroadcastMemoryLimit)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = operatorContext;
        this.partitionIndex = partitionIndex;
        this.filterFunctionFactory = filterFunctionFactory;
        this.sortChannel = sortChannel;
        this.searchFunctionFactories = searchFunctionFactories;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();

        // 创建Page的索引对象：PagesIndex
        this.index = pagesIndexFactory.newPagesIndex(lookupSourceFactory.getTypes(), expectedPositions);
        this.lookupSourceFactory = lookupSourceFactory;
        lookupSourceFactoryDestroyed = lookupSourceFactory.isDestroyed();

        this.outputChannels = outputChannels;
        this.hashChannels = hashChannels;
        this.preComputedHashChannel = preComputedHashChannel;

        // 创建Hash冲突计数器
        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);

        this.spillEnabled = spillEnabled;
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.enforceBroadcastMemoryLimit = enforceBroadcastMemoryLimit;
    }

    @VisibleForTesting
    public State getState()
    {
        return state;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        switch (state) {
            case CONSUMING_INPUT:
                return NOT_BLOCKED;

            case SPILLING_INPUT:
                return spillInProgress;

            case LOOKUP_SOURCE_BUILT:
                return lookupSourceNotNeeded.orElseThrow(() -> new IllegalStateException("Lookup source built, but disposal future not set"));

            case INPUT_SPILLED:
                return spilledLookupSourceHandle.getUnspillingOrDisposeRequested();

            case INPUT_UNSPILLING:
                return unspillInProgress.orElseThrow(() -> new IllegalStateException("Unspilling in progress, but unspilling future not set"));

            case INPUT_UNSPILLED_AND_BUILT:
                return spilledLookupSourceHandle.getDisposeRequested();

            case CLOSED:
                return NOT_BLOCKED;
        }
        throw new IllegalStateException("Unhandled state: " + state);
    }

    @Override
    public boolean needsInput()
    {
        boolean stateNeedsInput = (state == State.CONSUMING_INPUT)
                || (state == State.SPILLING_INPUT && spillInProgress.isDone());

        return stateNeedsInput && !lookupSourceFactoryDestroyed.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        // 如果探测侧已经销毁，则关闭
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        // 如果已经溢出了，则将Page写入磁盘
        if (state == State.SPILLING_INPUT) {
            spillInput(page);
            return;
        }

        checkState(state == State.CONSUMING_INPUT);
        // 更新索引
        updateIndex(page);
    }

    /**
     * 更新索引
     * @param page
     */
    private void updateIndex(Page page)
    {
        // 添加page
        index.addPage(page);

        if (spillEnabled) {
            localRevocableMemoryContext.setBytes(index.getEstimatedSize().toBytes());
        }
        else {
            if (!localUserMemoryContext.trySetBytes(index.getEstimatedSize().toBytes(), enforceBroadcastMemoryLimit)) {
                index.compact();
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes(), enforceBroadcastMemoryLimit);
            }
        }
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    private void spillInput(Page page)
    {
        checkState(spillInProgress.isDone(), "Previous spill still in progress");
        checkSpillSucceeded(spillInProgress);
        spillInProgress = getSpiller().spill(page);
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        checkState(spillEnabled, "Spill not enabled, no revokable memory should be reserved");

        if (state == State.CONSUMING_INPUT) {
            long indexSizeBeforeCompaction = index.getEstimatedSize().toBytes();
            index.compact();
            long indexSizeAfterCompaction = index.getEstimatedSize().toBytes();
            if (indexSizeAfterCompaction < indexSizeBeforeCompaction * INDEX_COMPACTION_ON_REVOCATION_TARGET) {
                finishMemoryRevoke = Optional.of(() -> {});
                return immediateFuture(null);
            }

            finishMemoryRevoke = Optional.of(() -> {
                index.clear();
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes(), enforceBroadcastMemoryLimit);
                localRevocableMemoryContext.setBytes(0);
                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
                state = State.SPILLING_INPUT;
            });
            return spillIndex();
        }
        else if (state == State.LOOKUP_SOURCE_BUILT) {
            finishMemoryRevoke = Optional.of(() -> {
                lookupSourceFactory.setPartitionSpilledLookupSourceHandle(partitionIndex, spilledLookupSourceHandle);
                lookupSourceNotNeeded = Optional.empty();
                index.clear();
                localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes(), enforceBroadcastMemoryLimit);
                localRevocableMemoryContext.setBytes(0);
                lookupSourceChecksum = OptionalLong.of(lookupSourceSupplier.checksum());
                lookupSourceSupplier = null;
                state = State.INPUT_SPILLED;
            });
            return spillIndex();
        }
        else if (operatorContext.getReservedRevocableBytes() == 0) {
            // Probably stale revoking request
            finishMemoryRevoke = Optional.of(() -> {});
            return immediateFuture(null);
        }

        throw new IllegalStateException(format("State %s can not have revocable memory, but has %s revocable bytes", state, operatorContext.getReservedRevocableBytes()));
    }

    private ListenableFuture<?> spillIndex()
    {
        checkState(!spiller.isPresent(), "Spiller already created");
        spiller = Optional.of(singleStreamSpillerFactory.create(
                index.getTypes(),
                operatorContext.getSpillContext().newLocalSpillContext(),
                operatorContext.newLocalSystemMemoryContext(HashBuilderOperator.class.getSimpleName())));
        return getSpiller().spill(index.getPages());
    }

    @Override
    public void finishMemoryRevoke()
    {
        checkState(finishMemoryRevoke.isPresent(), "Cannot finish unknown revoking");
        finishMemoryRevoke.get().run();
        finishMemoryRevoke = Optional.empty();
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    /**
     * 没有Page输入了。
     */
    @Override
    public void finish()
    {
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        if (finishMemoryRevoke.isPresent()) {
            return;
        }

        switch (state) {
            // 正在消费输入的Page的状态，但是因为当前是finish方法，所以这里是完成最后一个输入的Page
            case CONSUMING_INPUT:
                // 完成Page的输入，就可以通知LookUpOperator进行探测了。
                finishInput();
                return;

                // lookup的数据源构建完成
            case LOOKUP_SOURCE_BUILT:
                disposeLookupSourceIfRequested();
                return;

            case SPILLING_INPUT:
                finishSpilledInput();
                return;

            case INPUT_SPILLED:
                if (spilledLookupSourceHandle.getDisposeRequested().isDone()) {
                    close();
                }
                else {
                    unspillLookupSourceIfRequested();
                }
                return;

            case INPUT_UNSPILLING:
                finishLookupSourceUnspilling();
                return;

            case INPUT_UNSPILLED_AND_BUILT:
                disposeUnspilledLookupSourceIfRequested();
                return;

            case CLOSED:
                // no-op
                return;
        }

        throw new IllegalStateException("Unhandled state: " + state);
    }

    /**
     * 完成所有page的输入
     */
    private void finishInput()
    {
        checkState(state == State.CONSUMING_INPUT);
        // 再次判断如果探测端已经销毁，则关闭
        if (lookupSourceFactoryDestroyed.isDone()) {
            close();
            return;
        }

        // 构建LookupSourceSupplier -> 会创建JoinHashSupplier
        LookupSourceSupplier partition = buildLookupSource();
        // 如果可以写入磁盘，则设置可回收的内存size
        if (spillEnabled) {
            localRevocableMemoryContext.setBytes(partition.get().getInMemorySizeInBytes());
        }
        // 如果不能写入磁盘，则记录已使用的内存size
        else {
            localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes(), enforceBroadcastMemoryLimit);
        }
        // 不需要
        lookupSourceNotNeeded = Optional.of(lookupSourceFactory.lendPartitionLookupSource(partitionIndex, partition));

        // LookupSource已经构建并传递，没有发生任何泄漏
        state = State.LOOKUP_SOURCE_BUILT;
    }

    private void disposeLookupSourceIfRequested()
    {
        checkState(state == State.LOOKUP_SOURCE_BUILT);
        verify(lookupSourceNotNeeded.isPresent());
        if (!lookupSourceNotNeeded.get().isDone()) {
            return;
        }

        index.clear();
        localRevocableMemoryContext.setBytes(0);
        localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes(), enforceBroadcastMemoryLimit);
        lookupSourceSupplier = null;
        close();
    }

    private void finishSpilledInput()
    {
        checkState(state == State.SPILLING_INPUT);
        if (!spillInProgress.isDone()) {
            // Not ready to handle finish() yet
            return;
        }
        checkSpillSucceeded(spillInProgress);
        state = State.INPUT_SPILLED;
    }

    private void unspillLookupSourceIfRequested()
    {
        checkState(state == State.INPUT_SPILLED);
        if (!spilledLookupSourceHandle.getUnspillingRequested().isDone()) {
            // Nothing to do yet.
            return;
        }

        verify(spiller.isPresent());
        verify(!unspillInProgress.isPresent());

        localUserMemoryContext.setBytes(getSpiller().getSpilledPagesInMemorySize() + index.getEstimatedSize().toBytes(), enforceBroadcastMemoryLimit);
        unspillInProgress = Optional.of(getSpiller().getAllSpilledPages());

        state = State.INPUT_UNSPILLING;
    }

    private void finishLookupSourceUnspilling()
    {
        checkState(state == State.INPUT_UNSPILLING);
        if (!unspillInProgress.get().isDone()) {
            // Pages have not be unspilled yet.
            return;
        }

        // Use Queue so that Pages already consumed by Index are not retained by us.
        Queue<Page> pages = new ArrayDeque<>(getDone(unspillInProgress.get()));
        long memoryRetainedByRemainingPages = pages.stream()
                .mapToLong(Page::getRetainedSizeInBytes)
                .sum();
        localUserMemoryContext.setBytes(memoryRetainedByRemainingPages + index.getEstimatedSize().toBytes(), enforceBroadcastMemoryLimit);

        while (!pages.isEmpty()) {
            Page next = pages.remove();
            index.addPage(next);
            // There is no attempt to compact index, since unspilled pages are unlikely to have blocks with retained size > logical size.
            memoryRetainedByRemainingPages -= next.getRetainedSizeInBytes();
            localUserMemoryContext.setBytes(memoryRetainedByRemainingPages + index.getEstimatedSize().toBytes(), enforceBroadcastMemoryLimit);
        }

        LookupSourceSupplier partition = buildLookupSource();
        lookupSourceChecksum.ifPresent(checksum ->
                checkState(partition.checksum() == checksum, "Unspilled lookupSource checksum does not match original one"));
        localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes(), enforceBroadcastMemoryLimit);

        spilledLookupSourceHandle.setLookupSource(partition);

        state = State.INPUT_UNSPILLED_AND_BUILT;
    }

    private void disposeUnspilledLookupSourceIfRequested()
    {
        checkState(state == State.INPUT_UNSPILLED_AND_BUILT);
        if (!spilledLookupSourceHandle.getDisposeRequested().isDone()) {
            return;
        }

        index.clear();
        localUserMemoryContext.setBytes(index.getEstimatedSize().toBytes(), enforceBroadcastMemoryLimit);

        close();
    }

    /**
     * 构建LookupSource
     * @return
     */
    private LookupSourceSupplier buildLookupSource()
    {
        // 创建LookupSourceSupplier，创建JoinHashSupplier
        LookupSourceSupplier partition = index.createLookupSourceSupplier(operatorContext.getSession(), hashChannels, preComputedHashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.of(outputChannels));
        // 记录hash冲突
        hashCollisionsCounter.recordHashCollision(partition.getHashCollisions(), partition.getExpectedHashCollisions());
        checkState(lookupSourceSupplier == null, "lookupSourceSupplier is already set");
        this.lookupSourceSupplier = partition;
        return partition;
    }

    /**
     * 是否已经构建完成
     * @return
     */
    @Override
    public boolean isFinished()
    {
        //
        if (lookupSourceFactoryDestroyed.isDone()) {
            // Finish early when the probe side is empty
            close();
            return true;
        }

        return state == State.CLOSED;
    }

    private SingleStreamSpiller getSpiller()
    {
        return spiller.orElseThrow(() -> new IllegalStateException("Spiller not created"));
    }

    @Override
    public void close()
    {
        if (state == State.CLOSED) {
            return;
        }
        // close() can be called in any state, due for example to query failure, and must clean resource up unconditionally

        lookupSourceSupplier = null;
        state = State.CLOSED;
        finishMemoryRevoke = finishMemoryRevoke.map(ifPresent -> () -> {});

        try (Closer closer = Closer.create()) {
            closer.register(index::clear);
            spiller.ifPresent(closer::register);
            closer.register(() -> localUserMemoryContext.setBytes(0, enforceBroadcastMemoryLimit));
            closer.register(() -> localRevocableMemoryContext.setBytes(0));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
