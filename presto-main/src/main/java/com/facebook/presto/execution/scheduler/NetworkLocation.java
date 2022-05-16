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
package com.facebook.presto.execution.scheduler;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * 表示网络拓扑中的位置。假设位置是分层的，并且所有工作节点和拆分位置都应处于同一层次。
 *
 * Represents a location in the network topology. Locations are assumed to be hierarchical
 * and all worker nodes and split locations should be at the same level of the hierarchy.
 */
public final class NetworkLocation
{
    public static final NetworkLocation ROOT_LOCATION = new NetworkLocation();

    private final List<String> segments;

    public NetworkLocation(String... segments)
    {
        this(Arrays.asList(segments));
    }

    private NetworkLocation(List<String> segments)
    {
        this.segments = segments;
    }

    public static NetworkLocation create(List<String> segments)
    {
        requireNonNull(segments, "segments is null");
        return new NetworkLocation(ImmutableList.copyOf(segments));
    }

    public NetworkLocation subLocation(int start, int end)
    {
        return new NetworkLocation(segments.subList(start, end));
    }

    public List<String> getSegments()
    {
        return segments;
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
        NetworkLocation that = (NetworkLocation) obj;
        // NOTE: This is performance sensitive and does not use Objects.equals to avoid excess object allocation
        return segments.equals(that.segments);
    }

    @Override
    public int hashCode()
    {
        // NOTE: This is performance sensitive and does not use Objects.hash to avoid excess object allocation
        return segments.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("location", "/" + Joiner.on("/").join(segments))
                .toString();
    }
}
