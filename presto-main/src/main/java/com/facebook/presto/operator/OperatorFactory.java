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

import com.facebook.presto.execution.Lifespan;

/**
 * 算子工厂
 */
public interface OperatorFactory
{
    // 创建算子
    Operator createOperator(DriverContext driverContext);

    /**
     * 声明不再调用createOperator，并释放与此工厂关联的任何资源。
     *
     * Declare that createOperator will not be called any more and release
     * any resources associated with this factory.
     * <p>
     * This method will be called only once.
     * Implementation doesn't need to worry about duplicate invocations.
     * <p>
     * It is guaranteed that this will only be invoked after {@link #noMoreOperators(Lifespan)}
     * has been invoked for all applicable driver groups.
     */
    void noMoreOperators();

    /**
     * 声明在【指定的生命周期内】不再调用createOperator，并释放与此工厂关联的任何资源
     *
     * Declare that createOperator will not be called any more for the specified Lifespan,
     * and release any resources associated with this factory.
     * <p>
     * This method will be called only once for each Lifespan.
     * Implementation doesn't need to worry about duplicate invocations.
     * <p>
     * It is guaranteed that this method will be invoked for all applicable driver groups
     * before {@link #noMoreOperators()} is invoked.
     */
    default void noMoreOperators(Lifespan lifespan)
    {
        // do nothing
    }

    // 复制当前算子
    OperatorFactory duplicate();
}
