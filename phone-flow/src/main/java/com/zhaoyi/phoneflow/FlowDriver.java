package com.zhaoyi.phoneflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {
    public static void main(String[] args) throws Exception {
        // 0.检测参数
        if(args.length != 2){
            System.out.println("Please enter the parameter: data input and output paths...");
            System.exit(-1);
        }
        // 获取job
        Job job = Job.getInstance(new Configuration());

        // 设置jar路径
        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置分区类
//        job.setPartitionerClass(FlowPartition.class);
//        // 我们设置了5个分区，对应上。
//        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));



        System.exit(job.waitForCompletion(true)? 1:0);
    }
}
