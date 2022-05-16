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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.GroupByIdBlock;

/**
 * 分组累加器
 */
public interface GroupedAccumulator
{
    long getEstimatedSize();

    Type getFinalType();

    Type getIntermediateType();

    void addInput(GroupByIdBlock groupIdsBlock, Page page);

    void addIntermediate(GroupByIdBlock groupIdsBlock, Block block);

    void evaluateIntermediate(int groupId, BlockBuilder output);

    // 根据Aggregation Function不同，可能并不是最终的聚合结果，例如求average，需要汇总某个group key的所有的sum值和count值，
    // 最后sum除以count才是最终的平均值，所以这里的Final聚合结果计算就是要完成这样的工作：
    void evaluateFinal(int groupId, BlockBuilder output);

    //
    void prepareFinal();
}
