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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.WarningCollector;

import static java.util.Objects.requireNonNull;

/**
 * 连接器拆分管理器（我的理解：将数据切分成多个部分）
 *
 * SplitManger将table数据分块chunks,供Presto向workers分发处理。
 * 我的：一个文件或者表可以分成多个split，表示表/文件的多个数据。
 *
 * 例如：
 *
 * Hive connector 列举hive的每个partition的文件files，为每个文件建立一到多个split。
 * 对于没有分区（partitioned）的数据，一个相对好的策略是整个表作为一个split（Example HTTP connector 即为该类实现）
 */
public interface ConnectorSplitManager
{
    ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext);

    enum SplitSchedulingStrategy
    {
        UNGROUPED_SCHEDULING,
        GROUPED_SCHEDULING,
        REWINDABLE_GROUPED_SCHEDULING,
    }

    class SplitSchedulingContext
    {
        private final SplitSchedulingStrategy splitSchedulingStrategy;
        private final boolean schedulerUsesHostAddresses;
        private final WarningCollector warningCollector;

        /**
         * @param splitSchedulingStrategy the method by which splits are scheduled
         * @param schedulerUsesHostAddresses whether host addresses are take into account
         * when choosing where to schedule remotely accessible splits. If this is false,
         * the connector can return an empty list of addresses for remotely accessible
         * splits without any performance loss.  Non-remotely accessible splits always
         * need to provide host addresses.
         */
        public SplitSchedulingContext(SplitSchedulingStrategy splitSchedulingStrategy, boolean schedulerUsesHostAddresses, WarningCollector warningCollector)
        {
            this.splitSchedulingStrategy = requireNonNull(splitSchedulingStrategy, "splitSchedulingStrategy is null");
            this.schedulerUsesHostAddresses = schedulerUsesHostAddresses;
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null ");
        }

        public SplitSchedulingStrategy getSplitSchedulingStrategy()
        {
            return splitSchedulingStrategy;
        }

        public boolean schedulerUsesHostAddresses()
        {
            return schedulerUsesHostAddresses;
        }

        public WarningCollector getWarningCollector()
        {
            return warningCollector;
        }
    }
}
