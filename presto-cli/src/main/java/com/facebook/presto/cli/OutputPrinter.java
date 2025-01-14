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
package com.facebook.presto.cli;

import java.io.IOException;
import java.util.List;

/**
 * 输出打印器
 */
public interface OutputPrinter
{
    /**
     * 打印多行
     * @param rows
     * @param complete
     * @throws IOException
     */
    void printRows(List<List<?>> rows, boolean complete)
            throws IOException;

    void finish()
            throws IOException;
}
