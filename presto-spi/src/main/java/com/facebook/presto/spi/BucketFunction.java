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
package com.facebook.presto.spi;

import com.facebook.presto.common.Page;

/**
 * 桶函数，实现了固定的算法，可以筛选中某个实例节点
 */
public interface BucketFunction
{
    /**
     * 获取位于指定位置的元组的存储桶。注意，元组值可能为null。
     * Gets the bucket for the tuple at the specified position.
     * Note the tuple values may be null.
     */
    int getBucket(Page page, int position);
}
