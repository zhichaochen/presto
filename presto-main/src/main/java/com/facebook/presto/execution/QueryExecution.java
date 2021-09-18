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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.QueryTracker.TrackedQuery;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * 查询执行接口
 * 表示一次查询执行，启动、停止、管理、统计等
 */
public interface QueryExecution
        extends TrackedQuery
{
    // 当前查询状态
    QueryState getState();

    // 等待当前状态发生改变，当发生变化之后，会返回最新的状态。
    ListenableFuture<QueryState> getStateChange(QueryState currentState);

    // 添加状态变化监听器
    void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener);

    // 添加输出结果监听器
    void addOutputInfoListener(Consumer<QueryOutputInfo> listener);

    // 查询计划
    Plan getQueryPlan();

    // 基本的查询信息
    BasicQueryInfo getBasicQueryInfo();

    // 查询信息
    QueryInfo getQueryInfo();

    String getSlug();

    Duration getTotalCpuTime();

    DataSize getRawInputDataSize();

    DataSize getOutputDataSize();

    int getRunningTaskCount();

    DataSize getUserMemoryReservation();

    DataSize getTotalMemoryReservation();

    VersionedMemoryPoolId getMemoryPool();

    void setMemoryPool(VersionedMemoryPoolId poolId);

    void start();

    void cancelQuery();

    void cancelStage(StageId stageId);

    void recordHeartbeat();

    /**
     * 最终查询信息监听器（查询结果，或者中断等）
     * Add a listener for the final query info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener);

    /**
     * 查询执行工厂
     * @param <T>
     */
    interface QueryExecutionFactory<T extends QueryExecution>
    {
        T createQueryExecution(
                PreparedQuery preparedQuery,
                QueryStateMachine stateMachine,
                String slug,
                WarningCollector warningCollector,
                Optional<QueryType> queryType);
    }

    /**
     * Output schema and buffer URIs for query.  The info will always contain column names and types.  Buffer locations will always
     * contain the full location set, but may be empty.  Users of this data should keep a private copy of the seen buffers to
     * handle out of order events from the listener.  Once noMoreBufferLocations is set the locations will never change, and
     * it is guaranteed that all previously sent locations are contained in the buffer locations.
     */
    class QueryOutputInfo
    {
        private final List<String> columnNames;
        private final List<Type> columnTypes;
        private final Map<URI, TaskId> bufferLocations;
        private final boolean noMoreBufferLocations;

        public QueryOutputInfo(List<String> columnNames, List<Type> columnTypes, Map<URI, TaskId> bufferLocations, boolean noMoreBufferLocations)
        {
            this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
            this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.bufferLocations = ImmutableMap.copyOf(requireNonNull(bufferLocations, "bufferLocations is null"));
            this.noMoreBufferLocations = noMoreBufferLocations;
        }

        public List<String> getColumnNames()
        {
            return columnNames;
        }

        public List<Type> getColumnTypes()
        {
            return columnTypes;
        }

        public Map<URI, TaskId> getBufferLocations()
        {
            return bufferLocations;
        }

        public boolean isNoMoreBufferLocations()
        {
            return noMoreBufferLocations;
        }
    }
}
