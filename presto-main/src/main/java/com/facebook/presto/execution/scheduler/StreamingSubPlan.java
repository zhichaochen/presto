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

import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * 流式子计划
 * 为啥叫这个名字呢？各个阶段之间需要通过http请求进行数据传递，多个阶段之间会形成一条数据流
 *
 * 我理解：该类是为了形成包含RemoteSourceNode段与段之间的关系，因为当前类只包含一个字段-》fragment
 *
 * 相比SubPlan通过withBucketToPartition可以获取到分区方案
 *
 * StreamingSubPlan与SubPlan类似，但只包含流式子计划
 * StreamingSubPlan is similar to SubPlan but only contains streaming children
 */
public class StreamingSubPlan
{
    private final PlanFragment fragment; // 当前包含的计划段
    // streaming children
    private final List<StreamingSubPlan> children; // 当前段的子StreamingSubPlan

    public StreamingSubPlan(PlanFragment fragment, List<StreamingSubPlan> children)
    {
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.children = ImmutableList.copyOf(requireNonNull(children, "children is null"));
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public List<StreamingSubPlan> getChildren()
    {
        return children;
    }

    /**
     * 用桶去分区
     * @param bucketToPartition
     * @return
     */
    public StreamingSubPlan withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new StreamingSubPlan(fragment.withBucketToPartition(bucketToPartition), children);
    }
}
