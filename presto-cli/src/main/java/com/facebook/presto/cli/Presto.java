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

import static io.airlift.airline.SingleCommand.singleCommand;

/**
 * 入口类
 */
public final class Presto
{
    private Presto() {}

    // Console的run方法中，如果入参中含有--execute，会直接将值取出作为待执行的SQL语句。
    // 否则认为是通过--file指定了SQL文件
    public static void main(String[] args)
    {
        //
        Console console = singleCommand(Console.class).parse(args);

        // 显示帮助信息
        if (console.helpOption.showHelpIfRequested() ||
                console.versionOption.showVersionIfRequested()) {
            return;
        }

        // 运行
        System.exit(console.run() ? 0 : 1);
    }
}
