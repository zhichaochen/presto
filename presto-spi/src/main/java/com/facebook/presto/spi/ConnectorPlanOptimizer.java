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
package com.facebook.presto.spi;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;

/**
 * 连接器计划优化器
 * 给定一个PlanNode，返回一个转换后的的PlanNode
 *
 * Given a PlanNode, return a transformed PlanNode.
 * <p/>
 * The given {@param maxSubplan} is a highest PlanNode in the query plan, such that
 * (1) it is a PlanNode implementation in SPI (i.e., not an internal PlanNode), and
 * (2) all the TableScanNodes that are reachable from {@param maxSubplan} are from the current connector.
 * <p/>
 * There could be multiple PlanNodes satisfying the above conditions.
 * All of them will be processed with the given implementation of ConnectorPlanOptimizer.
 * Each optimization is processed exactly once at the end of logical planning (i.e. right before AddExchanges).
 */
public interface ConnectorPlanOptimizer
{
    /**
     * 返回的是下推后的执行计划树
     * @param maxSubplan
     * @param session
     * @param variableAllocator
     * @param idAllocator
     * @return
     */
    PlanNode optimize(
            PlanNode maxSubplan, //  maxSubplan // 可能是：除了output之外的其他计划
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator);
}
