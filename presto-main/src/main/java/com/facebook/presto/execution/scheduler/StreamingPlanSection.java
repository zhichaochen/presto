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

import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * 流计划部分
 *
 * 建立了
 *
 * 为啥叫流计划呢？
 *      因为： 一个阶段会将数据【拉取？/ 主动发送？】到另一个阶段，多个阶段的数据形成一条数据流
 * 为啥叫部分呢？
 *     因为不是所有的计划，仅仅包含了TableScanNode（物化阶段）和 RemoteSourceNode计划的阶段。不会有outputNode的阶段
 */
public class StreamingPlanSection
{
    private final StreamingSubPlan plan; // 流式子计划，建立了段和段之间的关系
    // materialized exchange children
    // 物化段的，包含TableScanNode的子部分阶段，要么是TableScanNode 要不是 RemoteSourceNode
    private final List<StreamingPlanSection> children; //

    public StreamingPlanSection(StreamingSubPlan plan, List<StreamingPlanSection> children)
    {
        this.plan = requireNonNull(plan, "plan is null");
        this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
    }

    public StreamingSubPlan getPlan()
    {
        return plan;
    }

    public List<StreamingPlanSection> getChildren()
    {
        return children;
    }

    /**
     *
     * @param subPlan
     * @return
     */
    public static StreamingPlanSection extractStreamingSections(SubPlan subPlan)
    {
        // 包含TableScanNode的SubPlan
        ImmutableList.Builder<SubPlan> materializedExchangeChildren = ImmutableList.builder();
        // 包含RemoteSourceNode的段
        StreamingSubPlan streamingSection = extractStreamingSection(subPlan, materializedExchangeChildren);

        return new StreamingPlanSection(
                streamingSection,
                materializedExchangeChildren.build().stream()
                        // 在这里也形成了递归的关系
                        .map(StreamingPlanSection::extractStreamingSections)
                        .collect(toImmutableList()));
    }

    /**
     * 提取流式片段
     * TODO 查找SubPlan中的所有ExchangeNode转化的节点
     * @param subPlan
     * @param materializedExchangeChildren
     * @return
     */
    private static StreamingSubPlan extractStreamingSection(SubPlan subPlan, ImmutableList.Builder<SubPlan> materializedExchangeChildren)
    {
        // StreamingSubPlan列表
        ImmutableList.Builder<StreamingSubPlan> streamingSources = ImmutableList.builder();
        // 查找子计划中包含RemoteSourceNode的片段ID
        Set<PlanFragmentId> streamingFragmentIds = subPlan.getFragment().getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        // 遍历子计划
        // TODO 分段是通过ExchangeNode进行分段的，ExchangeNode不是RemoteSourceNode，就是TableScanNode
        // 要不从其他Worker节点拉取数据的包含RemoteSourceNode阶段，要不就是包含TableScanNode查询数据库的阶段
        for (SubPlan child : subPlan.getChildren()) {
            // 递归查找RemoteSourceNode的
            if (streamingFragmentIds.contains(child.getFragment().getId())) {
                streamingSources.add(extractStreamingSection(child, materializedExchangeChildren));
            }
            else {
                // 物化的子计划
                materializedExchangeChildren.add(child);
            }
        }
        return new StreamingSubPlan(subPlan.getFragment(), streamingSources.build());
    }
}
