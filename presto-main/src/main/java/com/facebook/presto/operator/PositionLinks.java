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

import java.util.List;

/**
 * PositionLinks
 * 对于inner join语句，遍历一个表的数据时，会把另外一张表的所有相等的值都找到，所以在构建hash表时，
 * 就需要提前把这些相等的值都集中存储起来(类似其他的HashMap中使用链表来存储)，以便开始join时能快速找到这些相等的值来进行行连接。
 * Presto在处理这个问题时，使用了比较巧妙的方式，那就是使用positionLinks。如下图所示：
 *
 * 此类负责迭代生成行，这些行具有哈希列中的值与给定探测行中的值相同（但根据filterFunction可以在其他列上具有不匹配的值）。
 * This class is responsible for iterating over build rows, which have
 * same values in hash columns as given probe row (but according to
 * filterFunction can have non matching values on some other column).
 */
public interface PositionLinks
{
    long getSizeInBytes();

    /**
     * Initialize iteration over position links. Returns first potentially eligible
     * join position starting from (and including) position argument.
     * <p>
     * When there are no more position -1 is returned
     */
    int start(int position, int probePosition, Page allProbeChannelsPage);

    /**
     * Iterate over position links. When there are no more position -1 is returned.
     */
    int next(int position, int probePosition, Page allProbeChannelsPage);

    interface FactoryBuilder
    {
        /**
         * @return value that should be used in future references to created position links
         */
        int link(int left, int right);

        Factory build();

        /**
         * @return number of linked elements
         */
        int size();

        default boolean isEmpty()
        {
            return size() == 0;
        }
    }

    interface Factory
    {
        /**
         * Separate JoinFilterFunctions have to be created and supplied for each thread using PositionLinks
         * since JoinFilterFunction is not thread safe...
         */
        PositionLinks create(List<JoinFilterFunction> searchFunctions);

        /**
         * @return a checksum for this {@link PositionLinks}, useful when entity is restored from spilled data
         */
        long checksum();
    }
}
