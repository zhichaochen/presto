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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.builder.HashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.SpillableHashAggregationBuilder;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder.toTypes;
import static com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static com.facebook.presto.type.TypeUtils.NULL_HASH_CODE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

/**
 * Hash聚合算子，group by 操作主要通过该算子完成
 * 相同的group key可以聚合在一起，完成最终聚合操作
 */
public class HashAggregationOperator
        implements Operator
{
    private static final double MERGE_WITH_MEMORY_RATIO = 0.9;

    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> groupByTypes; // 分组字段的类型，因为可能有多个分组字段，所以用集合表示
        // 分组通道，这里说的通道即分组的字段在传入的字段的位置，用整型表示
        // 也就是对第几个block进行分组
        private final List<Integer> groupByChannels;
        private final List<Integer> globalAggregationGroupIds;
        private final Step step; // 聚合的步骤 主要有partial，即部分聚合；final，即最终聚合
        private final boolean produceDefaultOutput;
        private final List<AccumulatorFactory> accumulatorFactories; // 聚合器工厂，根据不同的聚合类型产生不同的聚合器
        //进行hash的通道，和分组通道类似，一般根据什么分组，就要根据什么字段组合进行hash
        // hashChannel中的数子就是表示要对输入的Page中的第几个Block（第几个字段）进行hash计算
        // 比如：[2, 4], 那么就会抽取Block2和Block4, 进行联合hash计算
        // 当然只要是hash计算就不能避免hash冲突，所以一点冲突，就需要使用这两个字段的具体值进行比较来解冲突，而聚合结果则一般默认是从联合hash值之后顺序存放。
        private final Optional<Integer> hashChannel;
        private final Optional<Integer> groupIdChannel;

        private final int expectedGroups;
        private final Optional<DataSize> maxPartialMemory;
        private final boolean spillEnabled;
        private final DataSize memoryLimitForMerge;
        private final DataSize memoryLimitForMergeWithMemory;
        private final SpillerFactory spillerFactory;
        private final JoinCompiler joinCompiler;
        private final boolean useSystemMemory;

        private boolean closed;

        @VisibleForTesting
        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    false,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    false,
                    new DataSize(0, MEGABYTE),
                    new DataSize(0, MEGABYTE),
                    (types, spillContext, memoryContext) -> {
                        throw new UnsupportedOperationException();
                    },
                    joinCompiler,
                    useSystemMemory);
        }

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                boolean produceDefaultOutput,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                SpillerFactory spillerFactory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    unspillMemoryLimit,
                    DataSize.succinctBytes((long) (unspillMemoryLimit.toBytes() * MERGE_WITH_MEMORY_RATIO)),
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory);
        }

        @VisibleForTesting
        HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                boolean produceDefaultOutput,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                boolean spillEnabled,
                DataSize memoryLimitForMerge,
                DataSize memoryLimitForMergeWithMemory,
                SpillerFactory spillerFactory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
            this.step = step;
            this.produceDefaultOutput = produceDefaultOutput;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.expectedGroups = expectedGroups;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
            this.spillEnabled = spillEnabled;
            this.memoryLimitForMerge = requireNonNull(memoryLimitForMerge, "memoryLimitForMerge is null");
            this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.useSystemMemory = useSystemMemory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName());
            HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
                    operatorContext,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory);
            return hashAggregationOperator;
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashAggregationOperatorFactory(
                    operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final List<Integer> globalAggregationGroupIds;
    private final Step step;
    private final boolean produceDefaultOutput;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;
    private final Optional<Integer> groupIdChannel;
    private final int expectedGroups;
    // 最大部分聚合内存，如果超过该阈值，会将其作为半成品传递到下个算子
    private final Optional<DataSize> maxPartialMemory;
    private final boolean spillEnabled;
    private final DataSize memoryLimitForMerge;
    private final DataSize memoryLimitForMergeWithMemory;
    private final SpillerFactory spillerFactory;
    private final JoinCompiler joinCompiler;
    private final boolean useSystemMemory;

    private final List<Type> types;
    private final HashCollisionsCounter hashCollisionsCounter;

    private HashAggregationBuilder aggregationBuilder; // hash聚合构建器
    private WorkProcessor<Page> outputPages;
    private boolean inputProcessed;
    private boolean finishing;
    private boolean finished;

    // for yield when memory is not available
    private Work<?> unfinishedWork;

    public HashAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> globalAggregationGroupIds,
            Step step,
            boolean produceDefaultOutput,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> hashChannel,
            Optional<Integer> groupIdChannel,
            int expectedGroups,
            Optional<DataSize> maxPartialMemory,
            boolean spillEnabled,
            DataSize memoryLimitForMerge,
            DataSize memoryLimitForMergeWithMemory,
            SpillerFactory spillerFactory,
            JoinCompiler joinCompiler,
            boolean useSystemMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(step, "step is null");
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");

        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
        this.step = step;
        this.produceDefaultOutput = produceDefaultOutput;
        this.expectedGroups = expectedGroups;
        this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
        this.spillEnabled = spillEnabled;
        this.memoryLimitForMerge = requireNonNull(memoryLimitForMerge, "memoryLimitForMerge is null");
        this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
        this.useSystemMemory = useSystemMemory;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || outputPages != null) {
            return false;
        }
        else if (aggregationBuilder != null && aggregationBuilder.isFull()) {
            return false;
        }
        else {
            return unfinishedWork == null;
        }
    }

    /**
     * 添加输入
     * 主要完成对一个Page数据的分组和partial聚合
     * @param page
     */
    @Override
    public void addInput(Page page)
    {
        checkState(unfinishedWork == null, "Operator has unfinished work");
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        inputProcessed = true;

        // 创建聚合构建器
        if (aggregationBuilder == null) {
            if (step.isOutputPartial() || !spillEnabled) {
                aggregationBuilder = new InMemoryHashAggregationBuilder(
                        accumulatorFactories,
                        step,
                        expectedGroups,
                        groupByTypes,
                        groupByChannels,
                        hashChannel,
                        operatorContext,
                        maxPartialMemory,
                        joinCompiler,
                        true,
                        useSystemMemory);
            }
            else {
                verify(!useSystemMemory, "using system memory in spillable aggregations is not supported");
                aggregationBuilder = new SpillableHashAggregationBuilder(
                        accumulatorFactories,
                        step,
                        expectedGroups,
                        groupByTypes,
                        groupByChannels,
                        hashChannel,
                        operatorContext,
                        memoryLimitForMerge,
                        memoryLimitForMergeWithMemory,
                        spillerFactory,
                        joinCompiler);
            }

            // assume initial aggregationBuilder is not full
        }
        else {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
        }

        // process the current page; save the unfinished work if we are waiting for memory
        // 处理当前的page，如果我们正在等待内存，请保存未完成的Work
        unfinishedWork = aggregationBuilder.processPage(page);
        // 开始处理工作，当处理完成会返回true
        if (unfinishedWork.process()) {
            unfinishedWork = null;
        }
        // 更新内存的使用情况
        aggregationBuilder.updateMemory();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        if (aggregationBuilder != null) {
            return aggregationBuilder.startMemoryRevoke();
        }
        return NOT_BLOCKED;
    }

    @Override
    public void finishMemoryRevoke()
    {
        if (aggregationBuilder != null) {
            aggregationBuilder.finishMemoryRevoke();
        }
    }

    @Override
    public Page getOutput()
    {
        // 如果完成则返回
        if (finished) {
            return null;
        }

        // process unfinished work if one exists
        // 处理未完成的工作
        if (unfinishedWork != null) {
            // 处理未完成的工作
            boolean workDone = unfinishedWork.process();
            // 更新内存使用情况
            aggregationBuilder.updateMemory();
            // 如果没有完成，则返回null
            if (!workDone) {
                return null;
            }
            unfinishedWork = null;
        }

        if (outputPages == null) {
            if (finishing) {
                if (!inputProcessed && produceDefaultOutput) {
                    // global aggregations always generate an output row with the default aggregation output (e.g. 0 for COUNT, NULL for SUM)
                    finished = true;
                    return getGlobalAggregationOutput();
                }

                if (aggregationBuilder == null) {
                    finished = true;
                    return null;
                }
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && (aggregationBuilder == null || !aggregationBuilder.isFull())) {
                return null;
            }

            outputPages = aggregationBuilder.buildResult();
        }

        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            closeAggregationBuilder();
            return null;
        }

        return outputPages.getResult();
    }

    @Override
    public void close()
    {
        closeAggregationBuilder();
    }

    @VisibleForTesting
    public HashAggregationBuilder getAggregationBuilder()
    {
        return aggregationBuilder;
    }

    private void closeAggregationBuilder()
    {
        outputPages = null;
        if (aggregationBuilder != null) {
            aggregationBuilder.recordHashCollisions(hashCollisionsCounter);
            aggregationBuilder.close();
            // aggregationBuilder.close() will release all memory reserved in memory accounting.
            // The reference must be set to null afterwards to avoid unaccounted memory.
            aggregationBuilder = null;
        }
        operatorContext.localUserMemoryContext().setBytes(0);
        operatorContext.localRevocableMemoryContext().setBytes(0);
    }

    /**
     *
     * @return
     */
    private Page getGlobalAggregationOutput()
    {
        // 遍历聚合器工厂列表，并创建聚合器
        List<Accumulator> accumulators = accumulatorFactories.stream()
                // No input will be added to the accumulators, it is ok not to specify the memory callback
                // 不会向累加器添加任何输入，可以不指定内存回调
                .map(accumulatorFactory -> accumulatorFactory.createAccumulator(UpdateMemory.NOOP))
                .collect(Collectors.toList());

        // global aggregation output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        // 全局聚合输出页将只构造一次，因此，构建了一个新的PageBuilder（而不是使用PageBuilder.reset）
        // 创建Page构造器
        PageBuilder output = new PageBuilder(globalAggregationGroupIds.size(), types);

        for (int groupId : globalAggregationGroupIds) {
            output.declarePosition();
            int channel = 0;

            for (; channel < groupByTypes.size(); channel++) {
                if (channel == groupIdChannel.get()) {
                    output.getBlockBuilder(channel).writeLong(groupId);
                }
                else {
                    output.getBlockBuilder(channel).appendNull();
                }
            }

            if (hashChannel.isPresent()) {
                long hashValue = calculateDefaultOutputHash(groupByTypes, groupIdChannel.get(), groupId);
                output.getBlockBuilder(channel++).writeLong(hashValue);
            }

            for (int j = 0; j < accumulators.size(); channel++, j++) {
                if (step.isOutputPartial()) {
                    accumulators.get(j).evaluateIntermediate(output.getBlockBuilder(channel));
                }
                else {
                    accumulators.get(j).evaluateFinal(output.getBlockBuilder(channel));
                }
            }
        }

        if (output.isEmpty()) {
            return null;
        }
        return output.build();
    }

    private static long calculateDefaultOutputHash(List<Type> groupByChannels, int groupIdChannel, int groupId)
    {
        // Default output has NULLs on all columns except of groupIdChannel
        long result = INITIAL_HASH_VALUE;
        for (int channel = 0; channel < groupByChannels.size(); channel++) {
            if (channel != groupIdChannel) {
                result = CombineHashFunction.getHash(result, NULL_HASH_CODE);
            }
            else {
                result = CombineHashFunction.getHash(result, BigintType.hash(groupId));
            }
        }
        return result;
    }
}
