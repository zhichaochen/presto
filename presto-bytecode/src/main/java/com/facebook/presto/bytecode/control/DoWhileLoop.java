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
package com.facebook.presto.bytecode.control;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.BytecodeVisitor;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * 生成 do while 循环 字节码，详细参考accept
 */
public class DoWhileLoop
        implements FlowControl
{
    // 备注
    private final String comment;
    // 字节码内容
    private final BytecodeBlock body = new BytecodeBlock();
    // 条件的字节码
    private final BytecodeBlock condition = new BytecodeBlock();

    // 开始的字节码位置
    private final LabelNode beginLabel = new LabelNode("begin");
    // continue字节码位置
    private final LabelNode continueLabel = new LabelNode("continue");
    // 结束的位置
    private final LabelNode endLabel = new LabelNode("end");

    public DoWhileLoop()
    {
        this.comment = null;
    }

    public DoWhileLoop(String format, Object... args)
    {
        this.comment = String.format(format, args);
    }

    @Override
    public String getComment()
    {
        return comment;
    }

    public LabelNode getContinueLabel()
    {
        return continueLabel;
    }

    public LabelNode getEndLabel()
    {
        return endLabel;
    }

    public BytecodeBlock body()
    {
        return body;
    }

    public DoWhileLoop body(BytecodeNode node)
    {
        checkState(body.isEmpty(), "body already set");
        body.append(node);
        return this;
    }

    public BytecodeBlock condition()
    {
        return condition;
    }

    public DoWhileLoop condition(BytecodeNode node)
    {
        checkState(condition.isEmpty(), "condition already set");
        condition.append(node);
        return this;
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        checkState(!condition.isEmpty(), "DoWhileLoop does not have a condition set");

        BytecodeBlock block = new BytecodeBlock()
                // 访问开始的label
                .visitLabel(beginLabel)
                // 拼接do中的内容
                .append(new BytecodeBlock()
                        .setDescription("body")
                        .append(body))
                .visitLabel(continueLabel)
                // 条件字节码
                .append(new BytecodeBlock()
                        .setDescription("condition")
                        .append(condition))
                // 如果报错字节跳转到循环结束位置
                .ifFalseGoto(endLabel)
                // 否则跳转到开始label
                .gotoLabel(beginLabel)
                // 访问结束label。
                .visitLabel(endLabel);

        block.accept(visitor, generationContext);
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of(body, condition);
    }

    @Override
    public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor)
    {
        return visitor.visitDoWhile(parent, this);
    }
}
