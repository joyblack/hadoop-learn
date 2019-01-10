package com.zhaoyi.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartition extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        char firstLetter = text.toString().charAt(0);
        if(Character.isLowerCase(firstLetter)){
            return 1;
        }
        return 0;
    }
}
