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
package com.facebook.presto.spi.function;

/**
 * Sql中的函数，函数的顶级接口，所有函数都实现该规范
 */
public interface SqlFunction
{
    // 函数签名
    Signature getSignature();

    // 函数可见性
    SqlFunctionVisibility getVisibility();

    //
    boolean isDeterministic();

    //
    boolean isCalledOnNullInput();

    // 函数描述
    String getDescription();
}
