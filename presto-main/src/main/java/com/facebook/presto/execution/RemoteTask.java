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
package com.facebook.presto.execution;

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;

import java.net.URI;

/**
 * 表示一个远程任务，可以与远程worker node进行交互
 * 通过RemoteSourceNode有什么关系呢？
 */
public interface RemoteTask
{
    TaskId getTaskId();

    String getNodeId();

    TaskInfo getTaskInfo();

    // 任务状态
    TaskStatus getTaskStatus();

    /**
     * 远程任务位置
     * TODO: this should be merged into getTaskStatus once full thrift support is in-place for v1/task
     */
    URI getRemoteTaskLocation();

    // 启动task
    void start();

    // 更新task，主要给task更新输入（splits）
    void addSplits(Multimap<PlanNodeId, Split> splits);

    // 设置当前任务没有split了
    void noMoreSplits(PlanNodeId sourceId);

    // TODO 告知远程的任务没有split了
    void noMoreSplits(PlanNodeId sourceId, Lifespan lifespan);

    void setOutputBuffers(OutputBuffers outputBuffers);

    ListenableFuture<?> removeRemoteSource(TaskId remoteSourceTaskId);

    /**
     * 添加任务状态改变监听器
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener);

    /**
     *
     * Add a listener for the final task info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener);

    ListenableFuture<?> whenSplitQueueHasSpace(int threshold);

    // 取消任务
    void cancel();

    // 终止任务
    void abort();

    int getPartitionedSplitCount();

    int getQueuedPartitionedSplitCount();

    int getUnacknowledgedPartitionedSplitCount();
}
