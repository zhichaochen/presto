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
package com.facebook.presto.operator.aggregation.builder;

import com.facebook.presto.common.Page;
import com.facebook.presto.operator.HashCollisionsCounter;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.WorkProcessor;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Hash聚合构建器
 */
public interface HashAggregationBuilder
        extends AutoCloseable
{
    // 处理每一页数据
    Work<?> processPage(Page page);

    // 构建结构
    WorkProcessor<Page> buildResult();

    // 是否溢出
    boolean isFull();

    // 更新内存的使用情况
    void updateMemory();

    // 记录哈希冲突
    void recordHashCollisions(HashCollisionsCounter hashCollisionsCounter);

    // 关闭
    @Override
    void close();

    // 开始内存回收
    ListenableFuture<?> startMemoryRevoke();

    // 完成内存回收
    void finishMemoryRevoke();
}
