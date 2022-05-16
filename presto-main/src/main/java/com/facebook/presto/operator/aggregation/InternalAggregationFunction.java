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

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * 内置的聚合函数信息
 */
public final class InternalAggregationFunction
{
    private final String name; // 函数名称
    private final List<Type> parameterTypes; // 函数的参数类型
    private final List<Type> intermediateType; // 中间类型
    private final Type finalType; // 最终类型
    private final List<Class> lambdaInterfaces; // lambda接口
    private final boolean decomposable; // 是否可分解
    private final boolean orderSensitive; // 是否顺序敏感
    private final AccumulatorFactoryBinder factory; // 聚合工厂绑定器

    public InternalAggregationFunction(
            String name,
            List<Type> parameterTypes,
            List<Type> intermediateType,
            Type finalType,
            boolean decomposable,
            boolean orderSensitive,
            AccumulatorFactoryBinder factory)
    {
        this(
                name,
                parameterTypes,
                intermediateType,
                finalType,
                decomposable,
                orderSensitive,
                factory,
                ImmutableList.of());
    }

    public InternalAggregationFunction(
            String name,
            List<Type> parameterTypes,
            List<Type> intermediateType,
            Type finalType,
            boolean decomposable,
            boolean orderSensitive,
            AccumulatorFactoryBinder factory,
            List<Class> lambdaInterfaces)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty(), "name is empty");
        this.parameterTypes = ImmutableList.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.decomposable = decomposable;
        this.orderSensitive = orderSensitive;
        this.factory = requireNonNull(factory, "factory is null");
        this.lambdaInterfaces = ImmutableList.copyOf(lambdaInterfaces);
    }

    public String name()
    {
        return name;
    }

    public List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    public Type getFinalType()
    {
        return finalType;
    }

    public Type getIntermediateType()
    {
        if (intermediateType.size() == 1) {
            return getOnlyElement(intermediateType);
        }
        else {
            return RowType.withDefaultFieldNames(intermediateType);
        }
    }

    public List<Class> getLambdaInterfaces()
    {
        return lambdaInterfaces;
    }

    /**
     * Indicates that the aggregation can be decomposed, and run as partial aggregations followed by a final aggregation to combine the intermediate results
     */
    public boolean isDecomposable()
    {
        return decomposable;
    }

    /**
     * Indicates that the aggregation is sensitive to input order
     */
    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }

    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel)
    {
        return factory.bind(
                inputChannels,
                maskChannel,
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                null,
                false,
                null,
                ImmutableList.of(),
                false,
                null);
    }

    public AccumulatorFactory bind(
            List<Integer> inputChannels,
            Optional<Integer> maskChannel,
            List<Type> sourceTypes,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            PagesIndex.Factory pagesIndexFactory,
            boolean distinct,
            JoinCompiler joinCompiler,
            List<LambdaProvider> lambdaProviders,
            boolean spillEnabled,
            Session session)
    {
        return factory.bind(inputChannels, maskChannel, sourceTypes, orderByChannels, orderings, pagesIndexFactory, distinct, joinCompiler, lambdaProviders, spillEnabled, session);
    }

    @VisibleForTesting
    public AccumulatorFactoryBinder getAccumulatorFactoryBinder()
    {
        return factory;
    }
}
