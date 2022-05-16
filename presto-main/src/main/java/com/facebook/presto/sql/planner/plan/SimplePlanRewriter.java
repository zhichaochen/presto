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

import java.util.List;

import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * 简单的计划重写器
 * @param <C>
 */
public abstract class SimplePlanRewriter<C>
        extends InternalPlanVisitor<PlanNode, SimplePlanRewriter.RewriteContext<C>>
{
    /**
     * 重写逻辑计划节点，这种写法，都是首先执行visitor#visitPlan方法
     *
     * @param rewriter
     * @param node
     * @param <C>
     * @return
     */
    public static <C> PlanNode rewriteWith(SimplePlanRewriter<C> rewriter, PlanNode node)
    {
        return node.accept(rewriter, new RewriteContext<>(rewriter, null));
    }

    public static <C> PlanNode rewriteWith(SimplePlanRewriter<C> rewriter, PlanNode node, C context)
    {
        return node.accept(rewriter, new RewriteContext<>(rewriter, context));
    }

    /**
     * 首先会执行这个方法
     * @param node
     * @param context
     * @return
     */
    @Override
    public PlanNode visitPlan(PlanNode node, RewriteContext<C> context)
    {
        return context.defaultRewrite(node, context.get());
    }

    /**
     * 重写上下文
     * @param <C>
     */
    public static class RewriteContext<C>
    {
        private final C userContext;
        private final SimplePlanRewriter<C> nodeRewriter;

        private RewriteContext(SimplePlanRewriter<C> nodeRewriter, C userContext)
        {
            this.nodeRewriter = nodeRewriter;
            this.userContext = userContext;
        }

        public C get()
        {
            return userContext;
        }

        /**
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children
         */
        public PlanNode defaultRewrite(PlanNode node)
        {
            return defaultRewrite(node, null);
        }

        /**
         * 默认重写：从逻辑计划的root节点开始，递归调用其子节点
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children
         *
         * 在给定节点的子节点上递归调用重写逻辑，并使用与重写子节点相同的副本将其交换出去
         */
        public PlanNode defaultRewrite(PlanNode node, C context)
        {
            // 重写当前节点的子节点
            List<PlanNode> children = node.getSources().stream()
                    .map(child -> rewrite(child, context))
                    .collect(toImmutableList());

            // 替换当前节点的子节点
            return replaceChildren(node, children);
        }

        /**
         * This method is meant for invoking the rewrite logic on children while processing a node
         */
        public PlanNode rewrite(PlanNode node, C userContext)
        {
            PlanNode result = node.accept(nodeRewriter, new RewriteContext<>(nodeRewriter, userContext));
            verify(result != null, "nodeRewriter returned null for %s", node.getClass().getName());

            return result;
        }

        /**
         * This method is meant for invoking the rewrite logic on children while processing a node
         */
        public PlanNode rewrite(PlanNode node)
        {
            return rewrite(node, null);
        }
    }
}
