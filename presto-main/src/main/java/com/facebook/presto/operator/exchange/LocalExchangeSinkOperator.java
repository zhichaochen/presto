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
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.LocalPlannerAware;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeFactory;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeSinkFactoryId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * sink：下沉，肯定还需要通过exchange传递到其他地方。
 * 在接收到Exchanger从网络接收到的一个page后，会对这个page的数据进行本地的repartition
 */
public class LocalExchangeSinkOperator
        implements Operator
{
    public static class LocalExchangeSinkOperatorFactory
            implements OperatorFactory, LocalPlannerAware
    {
        private final LocalExchangeFactory localExchangeFactory;

        private final int operatorId;
        // There will be a LocalExchangeSinkFactory per LocalExchangeSinkOperatorFactory per Driver Group.
        // A LocalExchangeSinkOperatorFactory needs to have access to LocalExchangeSinkFactories for each Driver Group.
        private final LocalExchangeSinkFactoryId sinkFactoryId;
        private final PlanNodeId planNodeId;
        private final Function<Page, Page> pagePreprocessor;
        private boolean closed;

        public LocalExchangeSinkOperatorFactory(LocalExchangeFactory localExchangeFactory, int operatorId, PlanNodeId planNodeId, LocalExchangeSinkFactoryId sinkFactoryId, Function<Page, Page> pagePreprocessor)
        {
            this.localExchangeFactory = requireNonNull(localExchangeFactory, "localExchangeFactory is null");

            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sinkFactoryId = requireNonNull(sinkFactoryId, "sinkFactoryId is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalExchangeSinkOperator.class.getSimpleName());

            LocalExchangeSinkFactory localExchangeSinkFactory = localExchangeFactory.getLocalExchange(driverContext.getLifespan()).getSinkFactory(sinkFactoryId);

            return new LocalExchangeSinkOperator(operatorContext, localExchangeSinkFactory.createSink(), pagePreprocessor);
        }

        @Override
        public void noMoreOperators()
        {
            if (!closed) {
                closed = true;
                localExchangeFactory.closeSinks(sinkFactoryId);
            }
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            localExchangeFactory.getLocalExchange(lifespan).getSinkFactory(sinkFactoryId).close();
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LocalExchangeSinkOperatorFactory(localExchangeFactory, operatorId, planNodeId, localExchangeFactory.newSinkFactoryId(), pagePreprocessor);
        }

        @Override
        public void localPlannerComplete()
        {
            localExchangeFactory.noMoreSinkFactories();
        }
    }

    private final OperatorContext operatorContext;
    private final LocalExchangeSink sink; //
    private final Function<Page, Page> pagePreprocessor; // page预处理函数
    private ListenableFuture<?> isBlocked = NOT_BLOCKED;

    LocalExchangeSinkOperator(OperatorContext operatorContext, LocalExchangeSink sink, Function<Page, Page> pagePreprocessor)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sink = requireNonNull(sink, "sink is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        sink.finish();
    }

    @Override
    public boolean isFinished()
    {
        return sink.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (isBlocked.isDone()) {
            isBlocked = sink.waitForWriting();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput()
    {
        return !isFinished() && isBlocked().isDone();
    }

    /**
     * 添加一个page
     * @param page
     */
    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        // 重新分区，调用了LocalExecutionPlanner#enforceLayoutProcessor
        page = pagePreprocessor.apply(page);
        // 将page交给一个exchanger，会将page，会调用LocalExchangeSource#addPage添加到LocalExchangeSource中。
        // 如果是分区hash，则会进行重分区，参考：LocalExchange#createPartitionFunction
        sink.addPage(page);
        // 记录算子输出
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
    {
        finish();
    }
}
