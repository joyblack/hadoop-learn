package com.zhaoyi.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void  main(String[] args) throws Exception {
        // 0.检测参数
        if(args.length != 2){
            System.out.println("Please enter the parameter: data input and output paths...");
            System.exit(-1);
        }
        // 1.根据配置信息创建任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2.设置驱动类
        job.setJarByClass(WordCountDriver.class);

        // 3.指定mapper和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4.设置输出结果的类型(reducer output)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5.设置输入数据路径和输出数据路径，由程序执行参数指定
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));

        // 设置分区类
//        job.setPartitionerClass(WordCountPartition.class);
//        job.setNumReduceTasks(2);
//        job.setCombinerClass(WordCountCombiner.class);

        // 6.提交工作
        //job.submit();
        boolean result = job.waitForCompletion(true);

        System.exit(result? 1:0);

    }
}
