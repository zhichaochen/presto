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

import com.facebook.presto.spi.PrestoException;

import java.util.concurrent.Future;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;

/**
 * 溢出工具类
 */
public class SpillingUtils
{
    private SpillingUtils() {}

    /**
     * 我们在空运中使用它而不是checkSuccess，这样我们可以传播错误消息，从而抛出一个PrestoException而不是一个IllegalArgumentException。
     * We use this instead of checkSuccess in airlift so we can propagate the error message
     * and so that we throw a PrestoException rather than an IllegalArgumentException.
     *
     * @param spillInProgress
     */
    public static void checkSpillSucceeded(Future spillInProgress)
    {
        try {
            getFutureValue(spillInProgress);
        }
        catch (PrestoException prestoException) {
            throw new PrestoException(prestoException::getErrorCode, prestoException.getMessage(), prestoException);
        }
        catch (RuntimeException runtimeException) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Spilling failed: %s", runtimeException.getMessage()), runtimeException);
        }
    }
}
