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
package com.facebook.presto.execution.scheduler;

import java.util.Collection;

/**
 * 所有阶段会在调度时一起调度, 所有阶段并发调度
 * 一旦数据可用，就会被立即进行处理。这种调度策略有利于那些对延迟敏的场景
 * 为啥呢？
 * 因为所有阶段已经被发送worker节点了，当上游查询出了数据，直接发送到下游阶段进行处理。
 */
public class AllAtOnceExecutionPolicy
        implements ExecutionPolicy
{
    @Override
    public ExecutionSchedule createExecutionSchedule(Collection<StageExecutionAndScheduler> stages)
    {
        return new AllAtOnceExecutionSchedule(stages);
    }
}
