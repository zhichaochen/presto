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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.common.Page;
import com.facebook.presto.operator.exchange.PageReference.PageReleasedListener;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * 广播算子，比如过滤之后，将过滤之后的数据广播到不同的节点，进行其他的操作。
 */
class BroadcastExchanger
        implements LocalExchanger
{
    // page的消费者逻辑, 向LocalExchangeSource中添加page。
    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager; // 内存管理器
    private final PageReleasedListener onPageReleased; // page释放监听器

    public BroadcastExchanger(List<Consumer<PageReference>> buffers, LocalExchangeMemoryManager memoryManager)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(buffers, "buffers is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.onPageReleased = PageReleasedListener.forLocalExchangeMemoryManager(memoryManager);
    }

    @Override
    public void accept(Page page)
    {
        memoryManager.updateMemoryUsage(page.getRetainedSizeInBytes());

        PageReference pageReference = new PageReference(page, buffers.size(), onPageReleased);

        for (Consumer<PageReference> buffer : buffers) {
            buffer.accept(pageReference);
        }
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
