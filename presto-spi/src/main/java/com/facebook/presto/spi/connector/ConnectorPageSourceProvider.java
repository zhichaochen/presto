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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;

import java.util.List;

/**
 * presto通过 ConnectorPageSourceProvider 或者 ConnectorRecordSetProvider 来获取数据
 * 两者可任选其一，其中ConnectorRecordSetProvider 适合数据量不大的查询
 * 实际上，Presto Core 也是通过 RecordPageSource 代理 RecordSet 的方式，把 ResourceSet数据集转为 Page的。
 *
 * page提供器，其中source表示
 * 会返回一个ConnectorPageSource对象，通过该对象可以从数据库中查询数据
 *
 */
public interface ConnectorPageSourceProvider
{
    /**
     * @param columns columns that should show up in the output page, in this order
     */
    @Deprecated
    default ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @param columns columns that should show up in the output page, in this order
     */
    default ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        return createPageSource(transactionHandle, session, split, columns, splitContext);
    }
}
