package com.zhaoyi.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输入K-V即为mapper的输出K-V类型，我们要的结果是word-count，因此输出K-V类型是Text-IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        // 1.汇总各个key的总数
        for (IntWritable value : values) {
            count += value.get();
        }
        // 2.输出该key的总数
        context.write(key, new IntWritable(count));
    }
}
