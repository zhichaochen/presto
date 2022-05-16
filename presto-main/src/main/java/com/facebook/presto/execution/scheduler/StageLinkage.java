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
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * 阶段连接，顾名思义，建立不同阶段之间的连接
 *
 * 通过该对象建立【不同阶段】的连接关系
 * TODO 比如：调度了一个阶段，在下一个阶段需要从上一个Stage获取数据，那么下个Stage肯定是需要从上一个阶段的节点去获取数据的。
 *
 * 其中：processScheduleResults ：当一个阶段完成之后，会调用该方法处理上一个调度器的调度结果
 * TODO 父子stage的区别：从数据库读取数据，数据的产生处，例如水流，就是上游，output：就是最上游，所谓的数据的最下游
 *
 */
public class StageLinkage
{
    private final PlanFragmentId currentStageFragmentId;// 当前阶段FragmentId
    /**
     * 参考SqlQuerySchedule#createStageExecutions
     * (fragmentId, tasks, noMoreExchangeLocations) ->
     *                         updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations);
     */
    // 调度成功子阶段后，在父阶段中更新子阶段所在的worker url，
    private final ExchangeLocationsConsumer parent; // 父阶段的消费逻辑
    private final Set<OutputBufferManager> childOutputBufferManagers; // 当前阶段的 子阶段 的所有输出缓存管理器

    /**
     * 创建TableScanNode
     * @param fragmentId // 当前段ID
     * @param parent
     * @param children ： // 当前段的【所有子段】的集合，SqlStageExecution是段信息的封装对象
     */
    public StageLinkage(PlanFragmentId fragmentId, ExchangeLocationsConsumer parent, Set<SqlStageExecution> children)
    {
        this.currentStageFragmentId = fragmentId;
        this.parent = parent;
        this.childOutputBufferManagers = children.stream()
                .map(childStage -> {
                    // 分区句柄
                    PartitioningHandle partitioningHandle = childStage.getFragment().getPartitioningScheme().getPartitioning().getHandle();
                    if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                        return new BroadcastOutputBufferManager(childStage::setOutputBuffers);
                    }
                    else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                        return new ScaledOutputBufferManager(childStage::setOutputBuffers);
                    }
                    else {
                        int partitionCount = Ints.max(childStage.getFragment().getPartitioningScheme().getBucketToPartition().get()) + 1;
                        return new PartitionedOutputBufferManager(partitioningHandle, partitionCount, childStage::setOutputBuffers);
                    }
                })
                .collect(toImmutableSet());
    }

    /**
     * TODO 处理调度结果
     * @param newState
     * @param newTasks
     */
    public void processScheduleResults(StageExecutionState newState, Set<RemoteTask> newTasks)
    {
        boolean noMoreTasks = false;
        switch (newState) {
            case PLANNED:
            case SCHEDULING:
                // workers are still being added to the query
                break;
            case FINISHED_TASK_SCHEDULING:
            case SCHEDULING_SPLITS:
            case SCHEDULED:
            case RUNNING:
            case FINISHED:
            case CANCELED:
                // no more workers will be added to the query
                noMoreTasks = true;
            case ABORTED:
            case FAILED:
                // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                // stage above to finish normally, which will result in a query
                // completing successfully when it should fail..
                break;
        }

        // Add an exchange location to the parent stage for each new task
        // 为每个新任务向父阶段添加exchange位置
        // TODO 对于非源头的stage，消费的是上游提供的remote split，而不是某个 connector split,
        //  所以需要在上游Stage调度时，获取其task的摆放位置，以此创建Remote Split，喂给下游 stage
        //  这样每个 task 就都知道去哪些上游 task pull 数据了.

        // 但是由于前文中提到的每个 ExecutionSchedule 的调度策略不同, 上游 stage 被调度时, 下游 task 可能都没有下发到 worker 上,
        // 这是会暂时把上游 task 的摆放信息先保存在 exchangeLocations 中, 等调度到下游 stage 后, 先检查其 exchangeLocations 成员,
        // 再将其初始化为 remote split 塞给其各个 task, 这样保证了不管调度顺序如何, 都不会发生 remote split 泄露.
        // 参考：https://zhuanlan.zhihu.com/p/58959725
        parent.addExchangeLocations(currentStageFragmentId, newTasks, noMoreTasks);

        // 子阶段的输出缓存不为空
        if (!childOutputBufferManagers.isEmpty()) {
            // Add an output buffer to the child stages for each new task
            // TODO 为每个新任务的子阶段添加一个输出缓冲区
            List<OutputBuffers.OutputBufferId> newOutputBuffers = newTasks.stream()
                    .map(task -> new OutputBuffers.OutputBufferId(task.getTaskId().getId()))
                    .collect(toImmutableList());
            for (OutputBufferManager child : childOutputBufferManagers) {
                child.addOutputBuffers(newOutputBuffers, noMoreTasks);
            }
        }
    }
}
