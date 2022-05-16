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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * 逻辑计划节点
 *
 * Presto IR（逻辑计划）的基本组成部分。
 * IR是一种树状结构，每个PlanNode执行特定的算子。
 *
 * The basic component of a Presto IR (logic plan).
 * An IR is a tree structure with each PlanNode performing a specific operation.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, property = "@type")
public abstract class PlanNode
{
    // 计划节点ID
    private final PlanNodeId id;

    protected PlanNode(PlanNodeId id)
    {
        requireNonNull(id, "id is null");
        this.id = id;
    }

    @JsonProperty("id")
    public PlanNodeId getId()
    {
        return id;
    }

    /**
     * 获取当前PlanNode的上游PlanNodes（即子节点）。
     * Get the upstream PlanNodes (i.e., children) of the current PlanNode.
     */
    public abstract List<PlanNode> getSources();

    /**
     * 输出内容
     * The output from the upstream PlanNodes.
     * It should serve as the input for the current PlanNode.
     */
    public abstract List<VariableReferenceExpression> getOutputVariables();

    /**
     * Alter the upstream PlanNodes of the current PlanNode.
     */
    public abstract PlanNode replaceChildren(List<PlanNode> newChildren);

    /**
     * 访问逻辑计划
     * 默认是访问计划，某些节点实现该类之后就是访问某个具体的节点
     * A visitor pattern interface to operate on IR.
     */
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
