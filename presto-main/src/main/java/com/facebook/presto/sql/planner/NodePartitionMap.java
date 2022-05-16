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
package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.FixedBucketNodeMap;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

/**
 * 节点分区Map
 * 其中：partitionToNode决定了会创建多少个task
 *
 * 当连接的探测端是bucket而构建器端不是bucket时，必须将bucket到分区的映射填充到构建器端的远程片段。
 * 这种情况下需要NodePartitionMap，不能简单地用BucketNodeMap替换。
 */
// When the probe side of join is bucketed but builder side is not,
// bucket to partition mapping has to be populated to builder side remote fragment.
// NodePartitionMap is required in this case and cannot be simply replaced by BucketNodeMap.
//
//      Join
//      /  \
//   Scan  Remote
//
// TODO: Investigate if we can use FixedBucketNodeMap and a node to taskId map to replace NodePartitionMap
//  in the above case, as the co-existence of BucketNodeMap and NodePartitionMap is confusing.
public class NodePartitionMap
{
    // 在调度的过程中，我们只需要知道有多少个桶就可以，至于关系，都是我们自己给对应起来的。
    // 分区到节点的映射，比如：获取第一个分区所在的节点， partitionToNode.get(0)即可，该关系在创建NodePartitionMap对象的时候进行维护的。
    private final List<InternalNode> partitionToNode;
    private final int[] bucketToPartition; // 桶到分区的映射
    private final ToIntFunction<Split> splitToBucket; // split 到 bucket的映射
    private final boolean cacheable; // 是否可以缓存

    public NodePartitionMap(List<InternalNode> partitionToNode, ToIntFunction<Split> splitToBucket)
    {
        this.partitionToNode = ImmutableList.copyOf(requireNonNull(partitionToNode, "partitionToNode is null"));
        this.bucketToPartition = IntStream.range(0, partitionToNode.size()).toArray();
        this.splitToBucket = requireNonNull(splitToBucket, "splitToBucket is null");
        this.cacheable = false;
    }

    public NodePartitionMap(List<InternalNode> partitionToNode, int[] bucketToPartition, ToIntFunction<Split> splitToBucket, boolean cacheable)
    {
        this.bucketToPartition = requireNonNull(bucketToPartition, "bucketToPartition is null");
        this.partitionToNode = ImmutableList.copyOf(requireNonNull(partitionToNode, "partitionToNode is null"));
        this.splitToBucket = requireNonNull(splitToBucket, "splitToBucket is null");
        this.cacheable = cacheable;
    }

    public List<InternalNode> getPartitionToNode()
    {
        return partitionToNode;
    }

    public int[] getBucketToPartition()
    {
        return bucketToPartition;
    }

    public InternalNode getNode(Split split)
    {
        int bucket = splitToBucket.applyAsInt(split);
        int partition = bucketToPartition[bucket];
        return requireNonNull(partitionToNode.get(partition));
    }

    public BucketNodeMap asBucketNodeMap()
    {
        ImmutableList.Builder<InternalNode> bucketToNode = ImmutableList.builder();
        for (int partition : bucketToPartition) {
            bucketToNode.add(partitionToNode.get(partition));
        }
        return new FixedBucketNodeMap(splitToBucket, bucketToNode.build(), cacheable);
    }
}
