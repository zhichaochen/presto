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
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.SetMultimap;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 节点映射
 * 用于存储presto集群中的节点信息
 */
public class NodeMap
{
    // 活跃节点map，key：节点ID，value：活跃的节点
    private final Map<String, InternalNode> activeNodesByNodeId;
    // 活跃工作节点，网路地址和内部映射
    private final SetMultimap<NetworkLocation, InternalNode> activeWorkersByNetworkPath;
    // 协调器节点ID
    private final Set<String> coordinatorNodeIds;
    // 活跃的节点列表
    private final List<InternalNode> activeNodes;
    // 所有节点列表
    private final List<InternalNode> allNodes;
    // Host和节点的映射
    private final SetMultimap<InetAddress, InternalNode> allNodesByHost;
    // Host、port和节点的映射
    private final SetMultimap<HostAddress, InternalNode> allNodesByHostAndPort;

    public NodeMap(
            Map<String, InternalNode> activeNodesByNodeId,
            SetMultimap<NetworkLocation, InternalNode> activeWorkersByNetworkPath,
            Set<String> coordinatorNodeIds,
            List<InternalNode> activeNodes,
            List<InternalNode> allNodes,
            SetMultimap<InetAddress, InternalNode> allNodesByHost,
            SetMultimap<HostAddress, InternalNode> allNodesByHostAndPort)
    {
        this.activeNodesByNodeId = activeNodesByNodeId;
        this.activeWorkersByNetworkPath = activeWorkersByNetworkPath;
        this.coordinatorNodeIds = coordinatorNodeIds;
        this.activeNodes = activeNodes;
        this.allNodes = allNodes;
        this.allNodesByHost = allNodesByHost;
        this.allNodesByHostAndPort = allNodesByHostAndPort;
    }

    public Map<String, InternalNode> getActiveNodesByNodeId()
    {
        return activeNodesByNodeId;
    }

    public SetMultimap<NetworkLocation, InternalNode> getActiveWorkersByNetworkPath()
    {
        return activeWorkersByNetworkPath;
    }

    public Set<String> getCoordinatorNodeIds()
    {
        return coordinatorNodeIds;
    }

    public List<InternalNode> getActiveNodes()
    {
        return activeNodes;
    }

    public List<InternalNode> getAllNodes()
    {
        return allNodes;
    }

    public SetMultimap<InetAddress, InternalNode> getAllNodesByHost()
    {
        return allNodesByHost;
    }

    public SetMultimap<HostAddress, InternalNode> getAllNodesByHostAndPort()
    {
        return allNodesByHostAndPort;
    }
}
