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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * join using 语句
 * 当左右表要关联的字段名称相同时，就可以使用using替换on语法，格式为：USING(column_name)。
 * 比如：订单表中的订单号列名order_code, 交付单表中的订单号列名也是order_code则可以使用Using
 *
 * INNER JOIN Order o ON f.order_code = o.order_code
 * 等价于
 * INNER JOIN Order o USING(order_code)
 */
public class JoinUsing
        extends JoinCriteria
{
    private final List<Identifier> columns;

    public JoinUsing(List<Identifier> columns)
    {
        requireNonNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "columns is empty");
        this.columns = ImmutableList.copyOf(columns);
    }

    public List<Identifier> getColumns()
    {
        return columns;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JoinUsing o = (JoinUsing) obj;
        return Objects.equals(columns, o.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(columns)
                .toString();
    }

    @Override
    public List<Node> getNodes()
    {
        return ImmutableList.of();
    }
}
