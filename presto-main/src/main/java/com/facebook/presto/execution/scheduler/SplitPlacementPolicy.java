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

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;

import java.util.List;
import java.util.Set;

/**
 * Split安置策略，给Split分配节点
 */
public interface SplitPlacementPolicy
{
    // TODO 为split分配节点
    SplitPlacementResult computeAssignments(Set<Split> splits);

    // 锁定下游节点
    void lockDownNodes();

    // 获取活跃的节点
    List<InternalNode> getActiveNodes();
}
