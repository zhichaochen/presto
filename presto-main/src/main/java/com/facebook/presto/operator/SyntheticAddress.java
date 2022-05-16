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
 * 创建和解码合成地址的方法。
 * 合成地址是Slices数组中的物理位置。地址已编码作为long，高32位包含数组中切片的索引，低32位包含数组中切片的索引在片内包含偏移量的位。
 *
 * Methods for creating and decoding synthetic addresses.
 * A synthetic address is a physical position within an array of Slices.  The address is encoded
 * as a long with the high 32 bits containing the index of the slice in the array and the low 32
 * bits containing an offset within the slice.
 */
public final class SyntheticAddress
{
    private SyntheticAddress()
    {
    }

    /**
     * 计算sliceAddress
     * @param sliceIndex ：
     * @param sliceOffset
     * @return
     */
    public static long encodeSyntheticAddress(int sliceIndex, int sliceOffset)
    {
        // 做偏移4个字节，int类型的长度，
        // 这样做能用一个long类型来表示两个int类型，那么通过获取long值得其前4位、后四位就能获取到sliceIndex、sliceOffset这两个值
        // 以8个字节举例：0011 和 0101，然后合并成00110101
        return (((long) sliceIndex) << 32) | sliceOffset;
    }

    public static int decodeSliceIndex(long sliceAddress)
    {
        return ((int) (sliceAddress >> 32));
    }

    public static int decodePosition(long sliceAddress)
    {
        // low order bits contain the raw offset, so a simple cast here will suffice
        return (int) sliceAddress;
    }
}
