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
package com.facebook.presto.spi.eventlistener;

/**
 * 事件监听器接口
 */
public interface EventListener
{
    // 查询创建相关事件
    default void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
    }

    // 查询完成相关事件
    default void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
    }

    // split执行信息，同理包含成功和失败的细节信息.
    default void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }
}
