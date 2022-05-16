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

// Lambda has to be compiled into a dedicated class, as functions might be stateful (e.g. use CachedInstanceBinder)
// Lambda必须编译成一个专用类，因为函数可能是有状态的（例如，使用CachedInstanceBinder）
public interface LambdaProvider
{
    // To support capture, we can enrich the interface into
    // getLambda(Object[] capturedValues)
    // 为了支持捕获，我们可以将接口扩展为getLambda(Object[]capturedValues)

    // The lambda capture is done through invokedynamic, and the CallSite will be cached after
    // the first call. Thus separate classes have to be generated for different captures.
    // lambda捕获是通过invokedynamic完成的，调用后将缓存调用站点第一个调用。因此，必须为不同的捕获生成单独的类。
    Object getLambda();
}
