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

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * 输入：表示一个数据库中表的信息
 */
@Immutable
public final class Input
{
    private final ConnectorId connectorId; // 连接器ID
    private final String schema; // 数据库
    private final String table; // 数据表
    private final List<Column> columns; // 数据表中的多列
    private final Optional<Object> connectorInfo; // 连接器信息
    private final Optional<TableStatistics> statistics; // 表统计信息

    @JsonCreator
    public Input(
            @JsonProperty("connectorId") ConnectorId connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("connectorInfo") Optional<Object> connectorInfo,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("statistics") Optional<TableStatistics> statistics)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.connectorInfo = requireNonNull(connectorInfo, "connectorInfo is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.statistics = requireNonNull(statistics, "table statistics is null");
    }

    @JsonProperty
    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<Object> getConnectorInfo()
    {
        return connectorInfo;
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<TableStatistics> getStatistics()
    {
        return statistics;
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
        Input input = (Input) o;
        return Objects.equals(connectorId, input.connectorId) &&
                Objects.equals(schema, input.schema) &&
                Objects.equals(table, input.table) &&
                Objects.equals(columns, input.columns) &&
                Objects.equals(connectorInfo, input.connectorInfo) &&
                Objects.equals(statistics, input.statistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schema, table, columns, connectorInfo, statistics);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(connectorId)
                .addValue(schema)
                .addValue(table)
                .addValue(columns)
                .addValue(statistics)
                .toString();
    }
}
