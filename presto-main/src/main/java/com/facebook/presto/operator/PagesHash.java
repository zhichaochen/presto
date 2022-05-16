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

import com.facebook.presto.array.AdaptiveLongBigArray;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.HashCommon;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * 计算pages的hash值 和 pagesIndex很类似
 *
 * ===========================
 * 1、key
 * 无论那种join都需要知道一个表中的值在另一个表中是否存在
 * key的大小就是hash表的大小
 * 每个位置存储的是一个字段的值的hash值对hashSize取模后的值
 *
 * 2、address：存储了每个值对应的位置
 *  当key值相等时，我们还需要做两类处理，
 *  第一是是否真的原始值就相等，如果原始值不等，那么说明hash冲突了，而要判断原始值是否相等，就是需要借助addresses来从积攒的一大堆数据中读取出原始值来判断；
 *  另外一个处理是原始值真的就相等了，那么就需要一个地方来存储这些相等的原始值在那一大推数据中的位置(就是address)，
 *  以便在inner join时把这些相等的值快速的找到并读出来，这就需要使用到positionLinks这个数组了。
 * 3、positionLinks
 */
// This implementation assumes arrays used in the hash are always a power of 2
public final class PagesHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesHash.class).instanceSize();
    private static final DataSize CACHE_SIZE = new DataSize(128, KILOBYTE);

    private final AdaptiveLongBigArray addresses; // 记录那个page的那一行
    private final int positionCount; // 总行数
    private final PagesHashStrategy pagesHashStrategy; // hash策略

    private final int channelCount; // 行数
    private final int mask; // 掩码
    // key的大小就是hash表的大小，是一个int数组，每个位置存储的是一个字段的值的hash值对hashSize取模后的值。
    private final int[] key; // 要关联的字段值
    private final long size; //

    // Native array of hashes for faster collisions resolution compared
    // to accessing values in blocks. We use bytes to reduce memory foot print
    // and there is no performance gain from storing full hashes
    // 与访问Block中的值相比，原生的哈希数组可以更快地解决冲突。
    // 我们使用字节来减少内存足迹，并且存储完整的散列不会带来性能提升
    private final byte[] positionToHashes; //
    private final long hashCollisions; // hash冲突的个数
    private final double expectedHashCollisions; // 预期hash冲突的个数

    /**
     * 构建PagesHash
     * @param addresses
     * @param positionCount
     * @param pagesHashStrategy
     * @param positionLinks
     */
    public PagesHash(
            AdaptiveLongBigArray addresses, // 类似数组
            int positionCount, // 位置总数
            PagesHashStrategy pagesHashStrategy, // page的Hash策略
            PositionLinks.FactoryBuilder positionLinks)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.positionCount = positionCount;
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();

        // reserve memory for the arrays
        // 预留数组的内存
        int hashSize = HashCommon.arraySize(positionCount, 0.75f);

        mask = hashSize - 1; // 掩码
        key = new int[hashSize]; //
        Arrays.fill(key, -1); // 给key数组填充-1

        positionToHashes = new byte[positionCount];

        // We will process addresses in batches, to save memory on array of hashes.
        // 我们将分批处理地址，以节省哈希数组上的内存。
        // ==========既然要分批读取，其中的step就表示一批
        // 在第几步中
        int positionsInStep = Math.min(positionCount + 1, (int) CACHE_SIZE.toBytes() / Integer.SIZE);
        long[] positionToFullHashes = new long[positionsInStep];
        long hashCollisionsLocal = 0;

        // 遍历所有step
        for (int step = 0; step * positionsInStep <= positionCount; step++) {
            // 每步的开始位置
            int stepBeginPosition = step * positionsInStep;
            // 每步的结束位置
            int stepEndPosition = Math.min((step + 1) * positionsInStep, positionCount);
            // step的长度
            int stepSize = stepEndPosition - stepBeginPosition;

            // First extract all hashes from blocks to native array.
            // Somehow having this as a separate loop is much faster compared
            // to extracting hashes on the fly in the loop below.
            // 首先将所有散列从块中提取到本机数组中。不知何故，将其作为一个单独的循环比在下面的循环中动态提取散列要快得多。
            // 遍历每一步中的
            for (int position = 0; position < stepSize; position++) {
                // 真正的位置
                int realPosition = position + stepBeginPosition;
                // hash位置
                long hash = readHashPosition(realPosition);
                //
                positionToFullHashes[position] = hash;
                positionToHashes[realPosition] = (byte) hash;
            }

            // index pages
            // 索引Pages
            for (int position = 0; position < stepSize; position++) {
                // 真正的位置
                int realPosition = position + stepBeginPosition;
                if (isPositionNull(realPosition)) {
                    continue;
                }

                // hash值
                long hash = positionToFullHashes[position];
                // 获取hash位置
                int pos = getHashPosition(hash, mask);

                // look for an empty slot or a slot containing this key
                // 查找空插槽或包含此key的插槽
                // key[pos]不为空
                while (key[pos] != -1) {
                    // 获取当前key
                    int currentKey = key[pos];
                    if (((byte) hash) == positionToHashes[currentKey] && positionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        realPosition = positionLinks.link(realPosition, currentKey);

                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                    hashCollisionsLocal++;
                }

                key[pos] = realPosition;
            }
        }

        // 占用内存大小
        size = addresses.getRetainedSizeInBytes() + pagesHashStrategy.getSizeInBytes() +
                sizeOf(key) + sizeOf(positionToHashes);
        // hash冲突数
        hashCollisions = hashCollisionsLocal;
        // 预计hash冲突数
        expectedHashCollisions = estimateNumberOfHashCollisions(positionCount, hashSize);
    }

    public final int getChannelCount()
    {
        return channelCount;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    public long getHashCollisions()
    {
        return hashCollisions;
    }

    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        return getAddressIndex(position, hashChannelsPage, pagesHashStrategy.hashRow(position, hashChannelsPage));
    }

    /**
     * LookupJoinOperator使用hash table的核心代码片段如下：
     * @param rightPosition
     * @param hashChannelsPage
     * @param rawHash : 对原始数据进行hash之后的值
     * @return
     */
    public int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash)
    {
        //
        int pos = getHashPosition(rawHash, mask);

        while (key[pos] != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(key[pos], (byte) rawHash, rightPosition, hashChannelsPage)) {
                return key[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.get(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.get(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.isPositionNull(blockIndex, blockPosition);
    }

    /**
     * hash位置
     * @param position
     * @return
     */
    private long readHashPosition(int position)
    {
        long pageAddress = addresses.get(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
    {
        if (positionToHashes[leftPosition] != rawHash) {
            return false;
        }

        long pageAddress = addresses.get(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.get(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.get(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return pagesHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }

    private static int getHashPosition(long rawHash, long mask)
    {
        // Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
        //
        // This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
        // Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
        // values returned by {@link Object#hashCode()}.
        //

        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;

        return (int) (rawHash & mask);
    }
}
