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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;

import java.util.List;

/**
 * 本类适用于数据量不大的查询，返回的 RecordSet 类似List
 * RecordSet 有个 InMemoryRecordSet 默认的实现，用于把返回的数据集直接放到内存List中。
 * 如果需要采用游标的方式获得数据需要自行实现 RecordSet 按照 batch 遍历数据。
 * 实际上，Presto Core 也是通过 RecordPageSource 代理 RecordSet 的方式，把 ResourceSet数据集转为 Page的。
 *
 * presto通过 ConnectorPageSourceProvider 或者 ConnectorRecordSetProvider 来获取数据
 *  * 两者可任选其一，其中ConnectorRecordSetProvider 适合数据量不大的查询
 *  * 实际上，Presto Core 也是通过 RecordPageSource 代理 RecordSet 的方式，把 ResourceSet数据集转为 Page的。
 *
 * 给定一个split和一组columns列表，RecordSetProvider负责将数据传输到Presto的执行引擎execution engine。
 * 它会构建一个RecordSet记录集，继而建立RecordCursor游标，供Presto行行读取各行中的列值（类似于JDBC）
 */
public interface ConnectorRecordSetProvider
{
    RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns);
}
