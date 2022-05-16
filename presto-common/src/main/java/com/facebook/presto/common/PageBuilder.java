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
package com.facebook.presto.common;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Page构建器，用于构建一个Page对象
 *
 * 不管是在Partial阶段还是在Final阶段，数据都会被积攒在内存中，达到一定的条件再输出
 * 在presto中，数据的输出都是通过写入PageBuilder来完成的，一个PageBuilder有一定的大小限制，
 * 而被积攒在内存中的数据可能已经非常多了，远远大于一个PageBuilder的大小
 */
public class PageBuilder
{
    // We choose default initial size to be 8 for PageBuilder and BlockBuilder
    // so the underlying data is larger than the object overhead, and the size is power of 2.
    //
    // This could be any other small number.
    private static final int DEFAULT_INITIAL_EXPECTED_ENTRIES = 8; // 默认的初始化预期条目

    private final BlockBuilder[] blockBuilders; // block构建器
    private final List<Type> types; // 多列也就是多个Block的数据类型
    private PageBuilderStatus pageBuilderStatus; // 状态
    private int declaredPositions; // 声明位置，当前page所处的位置

    /**
     * Create a PageBuilder with given types.
     * <p>
     * A PageBuilder instance created with this constructor has no estimation about bytes per entry,
     * therefore it can resize frequently while appending new rows.
     * <p>
     * This constructor should only be used to get the initial PageBuilder.
     * Once the PageBuilder is full use reset() or createPageBuilderLike() to create a new
     * PageBuilder instance with its size estimated based on previous data.
     */
    public PageBuilder(List<? extends Type> types)
    {
        this(DEFAULT_INITIAL_EXPECTED_ENTRIES, types);
    }

    public PageBuilder(int initialExpectedEntries, List<? extends Type> types)
    {
        this(initialExpectedEntries, DEFAULT_MAX_PAGE_SIZE_IN_BYTES, types, Optional.empty());
    }

    public static PageBuilder withMaxPageSize(int maxPageBytes, List<? extends Type> types)
    {
        return new PageBuilder(DEFAULT_INITIAL_EXPECTED_ENTRIES, maxPageBytes, types, Optional.empty());
    }

    private PageBuilder(int initialExpectedEntries, int maxPageBytes, List<? extends Type> types, Optional<BlockBuilder[]> templateBlockBuilders)
    {
        this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));

        pageBuilderStatus = new PageBuilderStatus(maxPageBytes);
        blockBuilders = new BlockBuilder[types.size()];

        // BlockBuilder模板存在，则使用模板
        if (templateBlockBuilders.isPresent()) {
            BlockBuilder[] templates = templateBlockBuilders.get();
            checkArgument(templates.length == types.size(), "Size of templates and types should match");
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = templates[i].newBlockBuilderLike(pageBuilderStatus.createBlockBuilderStatus());
            }
        }
        // 否则直接创建
        else {
            for (int i = 0; i < blockBuilders.length; i++) {
                blockBuilders[i] = types.get(i).createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), initialExpectedEntries);
            }
        }
    }

    public void reset()
    {
        if (isEmpty()) {
            return;
        }
        pageBuilderStatus = new PageBuilderStatus(pageBuilderStatus.getMaxPageSizeInBytes());

        declaredPositions = 0;

        for (int i = 0; i < types.size(); i++) {
            blockBuilders[i] = blockBuilders[i].newBlockBuilderLike(pageBuilderStatus.createBlockBuilderStatus());
        }
    }

    public PageBuilder newPageBuilderLike()
    {
        return new PageBuilder(declaredPositions, pageBuilderStatus.getMaxPageSizeInBytes(), types, Optional.of(blockBuilders));
    }

    public BlockBuilder getBlockBuilder(int channel)
    {
        return blockBuilders[channel];
    }

    public Type getType(int channel)
    {
        return types.get(channel);
    }

    public void declarePosition()
    {
        declaredPositions++;
    }

    public void declarePositions(int positions)
    {
        declaredPositions += positions;
    }

    public boolean isFull()
    {
        return declaredPositions == Integer.MAX_VALUE || pageBuilderStatus.isFull();
    }

    public boolean isEmpty()
    {
        return declaredPositions == 0;
    }

    public int getPositionCount()
    {
        return declaredPositions;
    }

    public long getSizeInBytes()
    {
        return pageBuilderStatus.getSizeInBytes();
    }

    public long getRetainedSizeInBytes()
    {
        // We use a foreach loop instead of streams
        // as it has much better performance.
        long retainedSizeInBytes = 0;
        for (BlockBuilder blockBuilder : blockBuilders) {
            retainedSizeInBytes += blockBuilder.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    /**
     * 构建 核心逻辑
     * @return
     */
    public Page build()
    {
        // 如果blockBuilders为0，则直接创建page
        if (blockBuilders.length == 0) {
            return new Page(declaredPositions);
        }

        // 创建block数组
        Block[] blocks = new Block[blockBuilders.length];
        // 遍历blocks, 多个blocks构建一个page
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockBuilders[i].build();
            if (blocks[i].getPositionCount() != declaredPositions) {
                throw new IllegalStateException(String.format("Declared positions (%s) does not match block %s's number of entries (%s)", declaredPositions, i, blocks[i].getPositionCount()));
            }
        }

        return Page.wrapBlocksWithoutCopy(declaredPositions, blocks);
    }

    private static void checkArgument(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
