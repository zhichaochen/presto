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
package com.facebook.presto.hive.pagefile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * 提到分布式架构的分区技术，不得不说说 Hadoop InputFormat，这个是 MapReduce 的基础。
 *
 * getSplits() 用于在任务启动时计算本次 MR 运行时切分逻辑。如：文件64M一个分片；HBase一个region 一个分片；
 * createRecordReader(split) 用于在运行时，把每个分片交给一个Task运行。实现分布式运行时数据读取；
 * split getLocations() 用于返回该分片数据的位置，用于Job调度时能就近调度。如任务运行在数据的节点上，这样可以减少网络开销。
 */
public class PageInputFormat
        extends FileInputFormat<NullWritable, NullWritable>
{
    @Override
    public RecordReader<NullWritable, NullWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
    {
        throw new UnsupportedOperationException();
    }

    protected boolean isSplitable(FileSystem fs, Path file)
    {
        return true;
    }
}
