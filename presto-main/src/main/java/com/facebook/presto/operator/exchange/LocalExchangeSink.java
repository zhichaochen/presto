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
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.facebook.presto.operator.exchange.LocalExchanger.FINISHED;
import static java.util.Objects.requireNonNull;

/**
 * 每个Source创建一个LocalExchangeSink
 * 作用是调用LocalExchanger#accept方法，本质上是将page，添加到LocalExchangeSource中，以便，LocalExchangeSourceOperator使用
 */
public class LocalExchangeSink
{
    /**
     * 当所有source都完成后，创建一个已完成的LocalExchangeSink
     * @return
     */
    public static LocalExchangeSink finishedLocalExchangeSink()
    {
        LocalExchangeSink finishedSink = new LocalExchangeSink(FINISHED, sink -> {});
        finishedSink.finish();
        return finishedSink;
    }

    private final LocalExchanger exchanger; // 交换器，比如：BroadcastExchanger
    private final Consumer<LocalExchangeSink> onFinish; // 当完成后做什么

    private final AtomicBoolean finished = new AtomicBoolean();

    public LocalExchangeSink(
            LocalExchanger exchanger,
            Consumer<LocalExchangeSink> onFinish)
    {
        this.exchanger = requireNonNull(exchanger, "exchanger is null");
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
    }

    public void finish()
    {
        if (finished.compareAndSet(false, true)) {
            exchanger.finish();
            onFinish.accept(this);
        }
    }

    public boolean isFinished()
    {
        return finished.get();
    }

    public void addPage(Page page)
    {
        requireNonNull(page, "page is null");

        // ignore pages after finished
        // this can happen with limit queries when all of the source (readers) are closed, so sinks
        // can be aborted early
        if (isFinished()) {
            return;
        }

        // there can be a race where finished is set between the check above and here
        // it is expected that the exchanger ignores pages after finish
        // 调用交换器的消费逻辑，本质来说会调用到LocalExchangeSource#addPage方法。
        exchanger.accept(page);
    }

    public ListenableFuture<?> waitForWriting()
    {
        if (isFinished()) {
            return NOT_BLOCKED;
        }
        return exchanger.waitForWriting();
    }
}
