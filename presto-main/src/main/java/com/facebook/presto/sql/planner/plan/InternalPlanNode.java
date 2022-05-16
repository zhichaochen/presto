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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanVisitor;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * 内部的计划节点
 * 增加了两个accept方法，子类重写了第二个accept方法，会接受一个Visitor来访问当前节点对应的方法，
 * 比如：TableFinishNode，便是调用访问器访问visitTableFinish()方法
 *
 *      @Override
 *     public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
 *     {
 *         return visitor.visitTableFinish(this, context);
 *     }
 *
 */
public abstract class InternalPlanNode
        extends PlanNode
{
    protected InternalPlanNode(PlanNodeId planNodeId)
    {
        super(planNodeId);
    }

    public final <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        checkArgument(visitor instanceof InternalPlanVisitor, "PlanVisitor is only for connector to use; InternalPlanNode should never use it");
        return accept((InternalPlanVisitor<R, C>) visitor, context);
    }

    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
