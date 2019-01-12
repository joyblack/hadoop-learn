package com.zhaoyi.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class MyDriver {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MyDriver.class);


        job.setOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);

        // 关联的自定义InputFormat
        job.setInputFormatClass(MyInputFormat.class);
        // 设置输出文件的格式为sequenceFile
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(MyMapper.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 1:0);
    }
}
