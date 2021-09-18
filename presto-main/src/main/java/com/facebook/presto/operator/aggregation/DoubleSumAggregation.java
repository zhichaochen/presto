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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

/**
 * double类型数据求和聚合
 */
@AggregationFunction("sum")
public final class DoubleSumAggregation
{
    private DoubleSumAggregation() {}

    /**
     * 完成对原始值的聚合(累加)
     * @param state
     * @param value
     */
    @InputFunction
    public static void sum(@AggregationState NullableDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(value);
        }
        else {
            state.setDouble(state.getDouble() + value);
        }
    }

    /**
     * 完成对两个不同中间状态的合并(相加)
     * @param state
     * @param otherState
     */
    @CombineFunction
    public static void combine(@AggregationState NullableDoubleState state, @AggregationState NullableDoubleState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(otherState.getDouble());
            return;
        }

        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    /**
     * 完成对最终状态的计算
     * @param state
     * @param out
     */
    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
    {
        NullableDoubleState.write(DoubleType.DOUBLE, state, out);
    }
}
