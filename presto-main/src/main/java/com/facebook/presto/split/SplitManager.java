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
package com.facebook.presto.split;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingContext;
import com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType.LEGACY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Split管理器
 */
public class SplitManager
{
    // Split集合，key：连接器ID，ConnectorSplitManager：不同的连接器管理器
    private final ConcurrentMap<ConnectorId, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();
    private final int minScheduleSplitBatchSize;
    private final Metadata metadata;
    private final boolean preferSplitHostAddresses;

    @Inject
    public SplitManager(MetadataManager metadata, QueryManagerConfig config, NodeSchedulerConfig nodeSchedulerConfig)
    {
        this.metadata = metadata;
        this.minScheduleSplitBatchSize = config.getMinScheduleSplitBatchSize();
        this.preferSplitHostAddresses = !nodeSchedulerConfig.getNetworkTopology().equals(LEGACY);
    }

    public void addConnectorSplitManager(ConnectorId connectorId, ConnectorSplitManager connectorSplitManager)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(connectorSplitManager, "connectorSplitManager is null");
        checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", connectorId);
    }

    public void removeConnectorSplitManager(ConnectorId connectorId)
    {
        splitManagers.remove(connectorId);
    }

    /**
     * 获取切分器
     * @param session
     * @param table
     * @param splitSchedulingStrategy
     * @param warningCollector
     * @return
     */
    public SplitSource getSplits(Session session, TableHandle table, SplitSchedulingStrategy splitSchedulingStrategy, WarningCollector warningCollector)
    {
        // 连接器ID
        ConnectorId connectorId = table.getConnectorId();
        // 通过连接器ID获取到连接器切换管理器
        ConnectorSplitManager splitManager = getConnectorSplitManager(connectorId);

        // 连接器Session
        ConnectorSession connectorSession = session.toConnectorSession(connectorId);
        // Now we will fetch the layout handle if it's not presented in TableHandle.
        // In the future, ConnectorTableHandle will be used to fetch splits since it will contain layout information.
        // 现在我们将获取布局句柄，如果它没有在TableHandle中呈现。
        // 将来，ConnectorTableHandle将用于获取拆分，因为它将包含布局信息。
        ConnectorTableLayoutHandle layout;
        if (!table.getLayout().isPresent()) {
            TableLayoutResult result = metadata.getLayout(session, table, Constraint.alwaysTrue(), Optional.empty());
            layout = result.getLayout().getLayoutHandle();
        }
        else {
            layout = table.getLayout().get();
        }

        // 连接器切分资源
        ConnectorSplitSource source = splitManager.getSplits(
                table.getTransaction(),
                connectorSession,
                layout,
                new SplitSchedulingContext(splitSchedulingStrategy, preferSplitHostAddresses, warningCollector));

        // 分割资源
        SplitSource splitSource = new ConnectorAwareSplitSource(connectorId, table.getTransaction(), source);
        if (minScheduleSplitBatchSize > 1) {
            splitSource = new BufferingSplitSource(splitSource, minScheduleSplitBatchSize);
        }
        return splitSource;
    }

    private ConnectorSplitManager getConnectorSplitManager(ConnectorId connectorId)
    {
        ConnectorSplitManager result = splitManagers.get(connectorId);
        checkArgument(result != null, "No split manager for connector '%s'", connectorId);
        return result;
    }
}
