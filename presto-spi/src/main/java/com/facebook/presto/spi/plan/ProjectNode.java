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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.plan.ProjectNode.Locality.UNKNOWN;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * Project：投影，映射的意思
 *
 * 用于进行映射的节点
 * 将下层的节点输出列映射成上层节点 例如：select a + 1 from b将TableScanNode的a列 + 1 映射到OutputNode
 *
 * 用于将Project Node下层节点输出的列映射到Project node上层节点输入的列。
 * 比如：select a + 1 from table，用于将a返回的值 + 1 后返回给上层。
 */
@Immutable
public final class ProjectNode
        extends PlanNode
{
    private final PlanNode source;
    private final Assignments assignments;
    private final Locality locality;

    public ProjectNode(PlanNodeId id, PlanNode source, Assignments assignments)
    {
        this(id, source, assignments, UNKNOWN);
    }

    // TODO: pass in the "assignments" and the "outputs" separately (i.e., get rid if the symbol := symbol idiom)
    @JsonCreator
    public ProjectNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("assignments") Assignments assignments,
            @JsonProperty("locality") Locality locality)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(assignments, "assignments is null");
        requireNonNull(locality, "locality is null");

        this.source = source;
        this.assignments = assignments;
        this.locality = locality;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return assignments.getOutputs();
    }

    @JsonProperty
    public Assignments getAssignments()
    {
        return assignments;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return singletonList(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public Locality getLocality()
    {
        return locality;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitProject(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        requireNonNull(newChildren, "newChildren list is null");
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("newChildren list has multiple items");
        }
        return new ProjectNode(getId(), newChildren.get(0), assignments, locality);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProjectNode that = (ProjectNode) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(assignments, that.assignments) &&
                locality == that.locality;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, assignments, locality);
    }

    /**
     * 位置
     */
    public enum Locality
    {
        UNKNOWN, // 位置
        LOCAL, // 本地
        REMOTE, // 远程
    }
}
