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

import com.facebook.presto.execution.SqlStageExecution;

import static java.util.Objects.requireNonNull;

/**
 * 其实表示一个可被调度器调度的阶段
 *
 * 该对象持有了阶段需要的数据 -》 SqlStageExecution
 * 同时持有了调度该阶段使用的调度器 -> StageScheduler
 */
public class StageExecutionAndScheduler
{
    private final SqlStageExecution stageExecution; // 阶段执行
    // 阶段连接（不同阶段通过该对象可以连接起来）
    private final StageLinkage stageLinkage;
    private final StageScheduler stageScheduler; // 阶段调度器（不同阶段对应不同调度器）

    StageExecutionAndScheduler(SqlStageExecution stageExecution, StageLinkage stageLinkage, StageScheduler stageScheduler)
    {
        this.stageExecution = requireNonNull(stageExecution, "stageExecution is null");
        this.stageLinkage = requireNonNull(stageLinkage, "stageLinkage is null");
        this.stageScheduler = requireNonNull(stageScheduler, "stageScheduler is null");
    }

    public SqlStageExecution getStageExecution()
    {
        return stageExecution;
    }

    public StageLinkage getStageLinkage()
    {
        return stageLinkage;
    }

    public StageScheduler getStageScheduler()
    {
        return stageScheduler;
    }
}
