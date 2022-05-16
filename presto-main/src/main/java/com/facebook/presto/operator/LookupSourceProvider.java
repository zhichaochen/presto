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

import java.util.function.Function;
import java.util.function.IntPredicate;

/**
 * Lookup需要的数据源提供器
 */
public interface LookupSourceProvider
        extends AutoCloseable
{
    <R> R withLease(Function<LookupSourceLease, R> action);

    @Override
    void close();

    /**
     * LookupSource 租户
     * 是一种维护分布式一致性的方式，在有效的期限内，给予一定的权限
     */
    interface LookupSourceLease
    {
        LookupSource getLookupSource();

        boolean hasSpilled();

        long spillEpoch();

        IntPredicate getSpillMask();
    }
}
