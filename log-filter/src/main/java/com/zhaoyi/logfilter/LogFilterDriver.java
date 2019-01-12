package com.zhaoyi.logfilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.UUID;

public class LogFilterDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(LogFilterDriver.class);
        job.setMapperClass(LogFilterMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置自定义输出类
        job.setOutputFormatClass(MyOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(LogFilterConfig.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(LogFilterConfig.OUTPUT_PATH + "_" +UUID.randomUUID()));


        System.exit(job.waitForCompletion(true)? 1:0);
    }
}
