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

import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * 查询状态
 */
@ThriftEnum
public enum QueryState
{
    /**
     * Query has been accepted and is awaiting execution.
     */
    QUEUED(false, 1),
    /**
     * Query is waiting for the required resources (beta).
     */
    WAITING_FOR_RESOURCES(false, 2),
    /**
     * Query is being dispatched to a coordinator.
     */
    DISPATCHING(false, 3),
    /**
     * Query is being planned.
     */
    PLANNING(false, 4),
    /**
     * Query execution is being started.
     */
    STARTING(false, 5),
    /**
     * Query has at least one running task.
     */
    RUNNING(false, 6),
    /**
     * Query is finishing (e.g. commit for autocommit queries)
     */
    FINISHING(false, 7),
    /**
     * Query has finished executing and all output has been consumed.
     */
    FINISHED(true, 8),
    /**
     * Query execution failed.
     */
    FAILED(true, 9);

    public static final Set<QueryState> TERMINAL_QUERY_STATES = Stream.of(QueryState.values()).filter(QueryState::isDone).collect(toImmutableSet());

    private final boolean doneState;
    private final int value;

    QueryState(boolean doneState, int value)
    {
        this.doneState = doneState;
        this.value = value;
    }

    /**
     * 终止状态
     * Is this a terminal state.
     */
    public boolean isDone()
    {
        return doneState;
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }
}
