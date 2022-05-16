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
package com.facebook.presto.operator;

/**
 * 表示一个JOB
 * 当函数调用需要在过程中产生yield/暂停时，使用该接口。用户需要调用{@link#process（）}，直到返回True后才能得到结果。
 *
 * The interface is used when a function call needs to yield/pause during the process.
 * Users need to call {@link #process()} until it returns True before getting the result.
 */
public interface Work<T>
{
    /**
     * 处理工作内容，调用方可以一直调用此方法，直到它返回true为止
     * Call the method to do the work.
     * The caller can keep calling this method until it returns true (i.e., the work is done).
     *
     * @return boolean to indicate if the work has finished. ： 表示工作是否完成
     */
    boolean process();

    /**
     * 获取处理结果
     * Get the result once the work is done.
     */
    T getResult();
}
