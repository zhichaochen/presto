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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;

/**
 * 数据定义任务，任务的相关实现。
 * @param <T>
 */
public interface DataDefinitionTask<T extends Statement>
{
    // 任务名
    String getName();

    // 任务执行
    ListenableFuture<?> execute(T statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters);

    // 解释Sql
    default String explain(T statement, List<Expression> parameters)
    {
        if (statement instanceof Prepare) {
            return SqlFormatter.formatSql(statement, Optional.empty());
        }

        return SqlFormatter.formatSql(statement, Optional.of(parameters));
    }

    // 是否有事务控制
    default boolean isTransactionControl()
    {
        return false;
    }
}
