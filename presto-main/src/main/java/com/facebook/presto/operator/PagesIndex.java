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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.array.AdaptiveLongBigArray;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.geospatial.Rectangle;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinCompiler.LookupSourceSupplierFactory;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

/**
 * 为Page建立的索引
 *
 * 通过ObjectArrayList<Block>[] channels;这个数据结构存储了多个page的信息，并提供了addPage，getPage方法。
 * ObjectArrayList是一个轻量级的list，底层也是通过数组实现的。
 *
 * 一个ObjectArrayList表示一列数据，添加page时，对应的block会添加到ObjectArrayList中，
 * 那么ObjectArrayList<Block>[]，就表示查询的多个page组成的所有数据。比如：
 *
 * 1、channel 讲解
 * channel0 channel1
 * -----------------
 * name，     age
 * ----------- page0    page index = 0
 * 张三，      21
 * 李四，      23
 *
 * ----------- page1   page index = 3
 * 王五，      22
 * 李留，      20
 * ----------
 * 2、positionCount、position
 *  表示一个page中的行数据位置，block.getPositionCount
 *  positionCount：表示总行数，在block中表示一个page中有多少行，在PagesIndex中表示总共有多少行。
 *  position ： 表示某一行
 *
 * 3、slice 色来s
 * 一个slice表示一片，表示查询数据中的一行
 * pageIndex * positionCount） + position，就可以计算出sliceAddress，也就是某一行数据所在的位置
 * 在记录当前值处于第几列，即可知道value所在位置
 * 也就是，知道了第几行 + 第几列，就定位到某个值的位置了。
 *
 * addPage便实现了这种关系的转换，并能快速查找到某行、某个值得位置。
 *
 * PagesIndex是一种低级数据结构，包含每个通道的每个值位置的地址。此数据结构不是通用的，设计用于一些特定用途：
 * PagesIndex a low-level data structure which contains the address of every value position of every channel.
 * This data structure is not general purpose and is designed for a few specific uses:
 * <ul>
 * <li>Sort via the {@link #sort} method</li>
 * <li>Hash build via the {@link #createLookupSourceSupplier} method</li>
 * <li>Positional output via the {@link #appendTo} method</li>
 * </ul>
 */
public class PagesIndex
        implements Swapper
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesIndex.class).instanceSize();
    private static final Logger log = Logger.get(PagesIndex.class);

    private final OrderingCompiler orderingCompiler;
    private final JoinCompiler joinCompiler;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final boolean groupByUsesEqualTo;

    private final List<Type> types;
    // 记录一行数据中一个字段值所在位置，（pageIndex * positionCount） + position  即可计算出，一个值所在位置
    private final AdaptiveLongBigArray valueAddresses;
    // 这是一个包含多个数组的List，该list包含多个Block数组，用于
    private final ObjectArrayList<Block>[] channels; // 表示多列数据，所有的page都会加入其中
    private final boolean eagerCompact; // 是否压缩
    private int nextBlockToCompact; //

    private int positionCount; // 记录总行数
    private long pagesMemorySize; // pages共占有多少内存
    private long estimatedSize; // 预计大小

    private PagesIndex(
            OrderingCompiler orderingCompiler,
            JoinCompiler joinCompiler,
            FunctionAndTypeManager functionAndTypeManager,
            boolean groupByUsesEqualTo,
            List<Type> types,
            int expectedPositions,
            boolean eagerCompact)
    {
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.groupByUsesEqualTo = groupByUsesEqualTo;
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.valueAddresses = new AdaptiveLongBigArray();
        this.valueAddresses.ensureCapacity(expectedPositions);
        this.eagerCompact = eagerCompact;

        //noinspection rawtypes
        // 初始化block数组列表
        channels = (ObjectArrayList<Block>[]) new ObjectArrayList[types.size()];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ObjectArrayList.wrap(new Block[1024], 0);
        }

        estimatedSize = calculateEstimatedSize();
    }

    public interface Factory
    {
        PagesIndex newPagesIndex(List<Type> types, int expectedPositions);
    }

    public static class TestingFactory
            implements Factory
    {
        private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler();
        private static final JoinCompiler JOIN_COMPILER = new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig());
        private final boolean groupByUsesEqualTo = new FeaturesConfig().isGroupByUsesEqualTo();
        private final boolean eagerCompact;

        public TestingFactory(boolean eagerCompact)
        {
            this.eagerCompact = eagerCompact;
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(ORDERING_COMPILER, JOIN_COMPILER, MetadataManager.createTestMetadataManager().getFunctionAndTypeManager(), groupByUsesEqualTo, types, expectedPositions, eagerCompact);
        }
    }

    public static class DefaultFactory
            implements Factory
    {
        private final OrderingCompiler orderingCompiler;
        private final JoinCompiler joinCompiler;
        private final boolean eagerCompact;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final boolean groupByUsesEqualTo;

        @Inject
        public DefaultFactory(OrderingCompiler orderingCompiler, JoinCompiler joinCompiler, FeaturesConfig featuresConfig, Metadata metadata)
        {
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.eagerCompact = requireNonNull(featuresConfig, "featuresConfig is null").isPagesIndexEagerCompactionEnabled();
            this.functionAndTypeManager = requireNonNull(metadata, "metadata is null").getFunctionAndTypeManager();
            this.groupByUsesEqualTo = featuresConfig.isGroupByUsesEqualTo();
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(orderingCompiler, joinCompiler, functionAndTypeManager, groupByUsesEqualTo, types, expectedPositions, eagerCompact);
        }
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public AdaptiveLongBigArray getValueAddresses()
    {
        return valueAddresses;
    }

    public ObjectArrayList<Block> getChannel(int channel)
    {
        return channels[channel];
    }

    public void clear()
    {
        for (ObjectArrayList<Block> channel : channels) {
            channel.clear();
            channel.trim();
        }
        valueAddresses.clear();
        positionCount = 0;
        nextBlockToCompact = 0;
        pagesMemorySize = 0;

        estimatedSize = calculateEstimatedSize();
    }

    /**
     * 添加page
     * @param page
     */
    public void addPage(Page page)
    {
        // ignore empty pages
        // 忽略空page
        if (page.getPositionCount() == 0) {
            return;
        }

        // page索引，通过该索引，可以计算出page的位置
        int pageIndex = (channels.length > 0) ? channels[0].size() : 0;
        // 遍历一个page中的所有列，并将其加入到对应的channel中
        for (int i = 0; i < channels.length; i++) {
            // 当前page的一列数组
            Block block = page.getBlock(i);
            // 期望压缩
            if (eagerCompact) {
                block = block.copyRegion(0, block.getPositionCount());
            }
            // 添加一个block
            channels[i].add(block);
            // 更新内存的使用情况
            pagesMemorySize += block.getRetainedSizeInBytes();
        }

        // 确保容量够用，不够的话会自动扩容
        valueAddresses.ensureCapacity(positionCount + page.getPositionCount());
        // 遍历page的多行数据，记录
        for (int position = 0; position < page.getPositionCount(); position++) {
            // slice地址，记录一行数组所在位置
            long sliceAddress = encodeSyntheticAddress(pageIndex, position);
            // 记录一行数据所在地址
            valueAddresses.set(positionCount, sliceAddress);
            positionCount++;
        }
        // 计算预计字节码
        estimatedSize = calculateEstimatedSize();
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(estimatedSize, BYTE);
    }

    public void compact()
    {
        if (eagerCompact) {
            return;
        }
        for (int channel = 0; channel < types.size(); channel++) {
            ObjectArrayList<Block> blocks = channels[channel];
            for (int i = nextBlockToCompact; i < blocks.size(); i++) {
                Block block = blocks.get(i);

                // Copy the block to compact its size
                Block compactedBlock = block.copyRegion(0, block.getPositionCount());
                blocks.set(i, compactedBlock);
                pagesMemorySize -= block.getRetainedSizeInBytes();
                pagesMemorySize += compactedBlock.getRetainedSizeInBytes();
            }
        }
        nextBlockToCompact = channels[0].size();
        estimatedSize = calculateEstimatedSize();
    }

    /**
     * 计算预计大小
     * @return
     */
    private long calculateEstimatedSize()
    {
        // 元素大小
        long elementsSize = (channels.length > 0) ? sizeOf(channels[0].elements()) : 0;
        // 通道数组大小
        long channelsArraySize = elementsSize * channels.length;
        //
        long addressesArraySize = valueAddresses.getRetainedSizeInBytes();
        // 当前类的实例大小 + page的大小 +
        return INSTANCE_SIZE + pagesMemorySize + channelsArraySize + addressesArraySize;
    }

    public Type getType(int channel)
    {
        return types.get(channel);
    }

    @Override
    public void swap(int a, int b)
    {
        valueAddresses.swap(a, b);
    }

    /**
     * 插入一个page数据
     * @param position
     * @param outputChannels
     * @param pageBuilder
     * @return
     */
    public int buildPage(int position, int[] outputChannels, PageBuilder pageBuilder)
    {
        while (!pageBuilder.isFull() && position < positionCount) {
            long pageAddress = valueAddresses.get(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            // append the row
            pageBuilder.declarePosition();
            for (int i = 0; i < outputChannels.length; i++) {
                int outputChannel = outputChannels[i];
                Type type = types.get(outputChannel);
                Block block = this.channels[outputChannel].get(blockIndex);
                type.appendTo(block, blockPosition, pageBuilder.getBlockBuilder(i));
            }

            position++;
        }

        return position;
    }

    public void appendTo(int channel, int position, BlockBuilder output)
    {
        long pageAddress = valueAddresses.get(position);

        Type type = types.get(channel);
        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        type.appendTo(block, blockPosition, output);
    }

    public boolean isNull(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return block.isNull(blockPosition);
    }

    public boolean getBoolean(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getBoolean(block, blockPosition);
    }

    public long getLong(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getLong(block, blockPosition);
    }

    public double getDouble(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getDouble(block, blockPosition);
    }

    public Slice getSlice(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getSlice(block, blockPosition);
    }

    public Object getObject(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getObject(block, blockPosition);
    }

    public Block getSingleValueBlock(int channel, int position)
    {
        long pageAddress = valueAddresses.get(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return block.getSingleValueBlock(blockPosition);
    }

    public void sort(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        sort(sortChannels, sortOrders, 0, getPositionCount());
    }

    public void sort(List<Integer> sortChannels, List<SortOrder> sortOrders, int startPosition, int endPosition)
    {
        createPagesIndexComparator(sortChannels, sortOrders).sort(this, startPosition, endPosition);
    }

    public boolean positionEqualsPosition(PagesHashStrategy partitionHashStrategy, int leftPosition, int rightPosition)
    {
        long leftAddress = valueAddresses.get(leftPosition);
        int leftPageIndex = decodeSliceIndex(leftAddress);
        int leftPagePosition = decodePosition(leftAddress);

        long rightAddress = valueAddresses.get(rightPosition);
        int rightPageIndex = decodeSliceIndex(rightAddress);
        int rightPagePosition = decodePosition(rightAddress);

        return partitionHashStrategy.positionEqualsPosition(leftPageIndex, leftPagePosition, rightPageIndex, rightPagePosition);
    }

    public boolean positionEqualsRow(PagesHashStrategy pagesHashStrategy, int indexPosition, int rightPosition, Page rightPage)
    {
        long pageAddress = valueAddresses.get(indexPosition);
        int pageIndex = decodeSliceIndex(pageAddress);
        int pagePosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRow(pageIndex, pagePosition, rightPosition, rightPage);
    }

    private PagesIndexOrdering createPagesIndexComparator(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        List<Type> sortTypes = sortChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
        return orderingCompiler.compilePagesIndexOrdering(sortTypes, sortChannels, sortOrders);
    }

    public Supplier<LookupSource> createLookupSourceSupplier(Session session, List<Integer> joinChannels)
    {
        return createLookupSourceSupplier(session, joinChannels, OptionalInt.empty(), Optional.empty(), Optional.empty(), ImmutableList.of());
    }

    public PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels, OptionalInt hashChannel)
    {
        return createPagesHashStrategy(joinChannels, hashChannel, Optional.empty());
    }

    public PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels, OptionalInt hashChannel, Optional<List<Integer>> outputChannels)
    {
        try {
            return joinCompiler.compilePagesHashStrategyFactory(types, joinChannels, outputChannels)
                    .createPagesHashStrategy(ImmutableList.copyOf(channels), hashChannel);
        }
        catch (Exception e) {
            log.error(e, "Lookup source compile failed for types=%s error=%s", types, e);
        }

        // if compilation fails, use interpreter
        return new SimplePagesHashStrategy(
                types,
                outputChannels.orElse(rangeList(types.size())),
                ImmutableList.copyOf(channels),
                joinChannels,
                hashChannel,
                Optional.empty(),
                functionAndTypeManager,
                groupByUsesEqualTo);
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            OptionalInt hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories)
    {
        return createLookupSourceSupplier(session, joinChannels, hashChannel, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.empty());
    }

    public PagesSpatialIndexSupplier createPagesSpatialIndex(
            Session session,
            int geometryChannel,
            Optional<Integer> radiusChannel,
            Optional<Integer> partitionChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            List<Integer> outputChannels,
            Map<Integer, Rectangle> partitions,
            LocalMemoryContext localUserMemoryContext)
    {
        // TODO probably shouldn't copy to reduce memory and for memory accounting's sake
        List<List<Block>> channels = ImmutableList.copyOf(this.channels);
        return new PagesSpatialIndexSupplier(session, valueAddresses, positionCount, types, outputChannels, channels, geometryChannel, radiusChannel, partitionChannel, spatialRelationshipTest, filterFunctionFactory, partitions, localUserMemoryContext);
    }

    /**
     * 创建LookupSourceSupplier
     */
    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            OptionalInt hashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            Optional<List<Integer>> outputChannels)
    {
        // 多列数据
        List<List<Block>> channels = ImmutableList.copyOf(this.channels);
        if (!joinChannels.isEmpty()) {
            // todo compiled implementation of lookup join does not support when we are joining with empty join channels.
            // This code path will trigger only for OUTER joins. To fix that we need to add support for
            //        OUTER joins into NestedLoopsJoin and remove "type == INNER" condition in LocalExecutionPlanner.visitJoin()
            // 这段代码，仅仅被OUTER joins所触发。要解决这个问题，我们需要添加对的支持
            try {
                LookupSourceSupplierFactory lookupSourceFactory = joinCompiler.compileLookupSourceFactory(types, joinChannels, sortChannel, outputChannels);
                return lookupSourceFactory.createLookupSourceSupplier(
                        session,
                        valueAddresses,
                        positionCount,
                        channels,
                        hashChannel,
                        filterFunctionFactory,
                        sortChannel,
                        searchFunctionFactories);
            }
            catch (Exception e) {
                log.error(e, "Lookup source compile failed for types=%s error=%s", types, e);
            }
        }

        // if compilation fails
        // 如果编译失败
        PagesHashStrategy hashStrategy = new SimplePagesHashStrategy(
                types,
                outputChannels.orElse(rangeList(types.size())),
                channels,
                joinChannels,
                hashChannel,
                sortChannel,
                functionAndTypeManager,
                groupByUsesEqualTo);

        return new JoinHashSupplier(
                session,
                hashStrategy,
                valueAddresses,
                positionCount,
                channels,
                filterFunctionFactory,
                sortChannel,
                searchFunctionFactories);
    }

    private List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", positionCount)
                .add("types", types)
                .add("estimatedSize", estimatedSize)
                .toString();
    }

    /**
     * 获取page，返回的是一个迭代器
     * @return
     */
    public Iterator<Page> getPages()
    {
        return new AbstractIterator<Page>()
        {
            // page计数器
            private int pageCounter;

            // 计算下一个page
            @Override
            protected Page computeNext()
            {
                // 如果计数器和
                if (pageCounter == channels[0].size()) {
                    return endOfData();
                }

                // 从channels中获取一个block
                Block[] blocks = Stream.of(channels)
                        .map(channel -> channel.get(pageCounter))
                        .toArray(Block[]::new);
                // page计数+1
                pageCounter++;
                // 返回一个page
                return new Page(blocks);
            }
        };
    }

    public Iterator<Page> getSortedPages()
    {
        return new AbstractIterator<Page>()
        {
            private int currentPosition;
            private final PageBuilder pageBuilder = new PageBuilder(types);
            private final int[] outputChannels = new int[types.size()];

            {
                Arrays.setAll(outputChannels, IntUnaryOperator.identity());
            }

            @Override
            public Page computeNext()
            {
                currentPosition = buildPage(currentPosition, outputChannels, pageBuilder);
                if (pageBuilder.isEmpty()) {
                    return endOfData();
                }
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return page;
            }
        };
    }
}
