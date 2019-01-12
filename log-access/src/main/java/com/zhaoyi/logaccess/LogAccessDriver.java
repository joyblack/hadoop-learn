package com.zhaoyi.logaccess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.UUID;

public class LogAccessDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(LogAccessDriver.class);
        job.setMapperClass(LogAccessMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(MyConfig.INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(MyConfig.OUTPUT_PATH));

        System.exit(job.waitForCompletion(true)? 1:0);
    }
}
