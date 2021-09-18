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
package com.facebook.presto.operator.project;

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.spi.RecordCursor;

/**
 * 游标处理器：估计是为了遍历
 *
 * 一个没有实现类的接口，动态生成代码
 */
public interface CursorProcessor
{
    CursorProcessorOutput process(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, RecordCursor cursor, PageBuilder pageBuilder);
}
