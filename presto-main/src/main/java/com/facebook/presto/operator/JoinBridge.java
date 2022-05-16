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

import com.google.common.util.concurrent.ListenableFuture;

/**
 * 连接构建、探测、外部join算子的桥，它通常携带数据让什么事可用的构建端，让外部查找孤立行
 *
 * A bridge that connects build, probe, and outer operators of a join.
 * It often carries data that lets probe find out what is available on
 * the build side, and lets outer find the orphaned rows.
 */
public interface JoinBridge
{
    /**
     * 只能在生成和探测完成后调用。
     * Can be called only after build and probe are finished.
     */
    OuterPositionIterator getOuterPositionIterator();

    // 销毁
    void destroy();

    /**
     * 当构建完成
     * @return
     */
    ListenableFuture<?> whenBuildFinishes();
}
