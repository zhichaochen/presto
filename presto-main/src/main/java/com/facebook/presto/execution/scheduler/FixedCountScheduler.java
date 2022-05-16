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

import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.metadata.InternalNode;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * 用于调度remote split，remote split是一种
 */
public class FixedCountScheduler
        implements StageScheduler
{
    public interface TaskScheduler
    {
        Optional<RemoteTask> scheduleTask(InternalNode node, int partition);
    }

    private final TaskScheduler taskScheduler; // 任务调度器，最终去调用SqlStageExecution#scheduleTask
    private final List<InternalNode> partitionToNode; // 分区和节点的映射

    public FixedCountScheduler(SqlStageExecution stage, List<InternalNode> partitionToNode)
    {
        requireNonNull(stage, "stage is null");
        this.taskScheduler = stage::scheduleTask;
        this.partitionToNode = requireNonNull(partitionToNode, "partitionToNode is null");
    }

    @VisibleForTesting
    public FixedCountScheduler(TaskScheduler taskScheduler, List<InternalNode> partitionToNode)
    {
        this.taskScheduler = requireNonNull(taskScheduler, "taskScheduler is null");
        this.partitionToNode = requireNonNull(partitionToNode, "partitionToNode is null");
    }

    /**
     * 调度, 由这里可知，有多少分区，就会生成多少个任务
     * @return
     */
    @Override
    public ScheduleResult schedule()
    {
        // 调度任务
        List<RemoteTask> newTasks = IntStream.range(0, partitionToNode.size())
                // 调度任务
                .mapToObj(partition -> taskScheduler.scheduleTask(partitionToNode.get(partition), partition))
                // Optional<RemoteTask>是否存在
                .filter(Optional::isPresent)
                // 如果存在则返回RemoteTask
                .map(Optional::get)
                .collect(toImmutableList());

        // no need to call stage.transitionToSchedulingSplits() since there is no table splits
        // 不需要调用stage.transitionToSchedulingSplits()，因为这里时没有表的splits
        return ScheduleResult.nonBlocked(true, newTasks, 0);
    }
}
