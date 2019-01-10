package com.zhaoyi.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 4个参数分别对应指定输入k-v类型以及输出k-v类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. transport the Text to Java String, this is a line.
        String line = value.toString();
        // 2. split to the line by " "
        String[] words = line.split(" ");
        // 3. output the word-1 key-val to context.
        for (String word:words) {
            // set word as key，number 1 as value
            // 根据单词分发，以便于相同单词会到相同的reducetask中
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
