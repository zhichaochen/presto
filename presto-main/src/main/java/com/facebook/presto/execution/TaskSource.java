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
package com.facebook.presto.execution;

import com.facebook.presto.spi.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * 任务来源，表示一个任务执行的数据源
 * 其中：planNodeId：决定了那个计划节点的任务， splits ： 表示该任务执行的数据分片， noMoreSplits ： 表示是否还有更多的任务分片需要调度
 *
 * 协调器Coordinator创建TaskSource， Worker使用TaskSource
 * 会将该对象从协调节点发送到worker节点，协调器Coordinator 和 Worker两边都会用到
 *
 * 参考：https://zhuanlan.zhihu.com/p/57866550
 */
public class TaskSource
{
    // worker 节点ID
    private final PlanNodeId planNodeId;
    // 切片集合，本次任务要处理的所有切片
    private final Set<ScheduledSplit> splits;
    // splits的生命周期
    private final Set<Lifespan> noMoreSplitsForLifespan;
    // true : 表示没有splits了
    private final boolean noMoreSplits;

    @JsonCreator
    public TaskSource(
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("splits") Set<ScheduledSplit> splits,
            @JsonProperty("noMoreSplitsForLifespan") Set<Lifespan> noMoreSplitsForLifespan,
            @JsonProperty("noMoreSplits") boolean noMoreSplits)
    {
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.splits = ImmutableSet.copyOf(requireNonNull(splits, "splits is null"));
        this.noMoreSplitsForLifespan = ImmutableSet.copyOf(noMoreSplitsForLifespan);
        this.noMoreSplits = noMoreSplits;
    }

    public TaskSource(PlanNodeId planNodeId, Set<ScheduledSplit> splits, boolean noMoreSplits)
    {
        this(planNodeId, splits, ImmutableSet.of(), noMoreSplits);
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public Set<ScheduledSplit> getSplits()
    {
        return splits;
    }

    @JsonProperty
    public Set<Lifespan> getNoMoreSplitsForLifespan()
    {
        return noMoreSplitsForLifespan;
    }

    @JsonProperty
    public boolean isNoMoreSplits()
    {
        return noMoreSplits;
    }

    /**
     * 更新TaskSource
     * @param source
     * @return
     */
    public TaskSource update(TaskSource source)
    {
        checkArgument(planNodeId.equals(source.getPlanNodeId()), "Expected source %s, but got source %s", planNodeId, source.getPlanNodeId());

        // 是否需要创建新的TaskSource
        if (isNewer(source)) {
            // assure the new source is properly formed
            // we know that either the new source one has new splits and/or it is marking the source as closed
            checkArgument(!noMoreSplits || splits.containsAll(source.getSplits()), "Source %s has new splits, but no more splits already set", planNodeId);

            // 分片列表
            Set<ScheduledSplit> newSplits = ImmutableSet.<ScheduledSplit>builder()
                    .addAll(splits)
                    .addAll(source.getSplits())
                    .build();
            //
            Set<Lifespan> newNoMoreSplitsForDriverGroup = ImmutableSet.<Lifespan>builder()
                    .addAll(noMoreSplitsForLifespan)
                    .addAll(source.getNoMoreSplitsForLifespan())
                    .build();

            return new TaskSource(
                    planNodeId,
                    newSplits,
                    newNoMoreSplitsForDriverGroup,
                    source.isNoMoreSplits());
        }
        else {
            // the specified source is older than this one
            return this;
        }
    }

    /**
     * 是否是新创建任务
     * @param source
     * @return
     */
    private boolean isNewer(TaskSource source)
    {
        // the specified source is newer if it changes the no more
        // splits flag or if it contains new splits
        return (!noMoreSplits && source.isNoMoreSplits()) ||
                (!noMoreSplitsForLifespan.containsAll(source.getNoMoreSplitsForLifespan())) ||
                (!splits.containsAll(source.getSplits()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("planNodeId", planNodeId)
                .add("splits", splits)
                .add("noMoreSplits", noMoreSplits)
                .toString();
    }
}
