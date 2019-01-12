package com.zhaoyi.logaccess;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogAccessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 判断是否是正常记录，正常记录不予输出，只输出不正常的访问记录
        if(isNormalRecord(value.toString(),context)){
            return;
        }else{
            // 记录下来
            context.write(value, NullWritable.get());
        }
    }

    private boolean isNormalRecord(String record, Context context){
        if(record.startsWith(MyConfig.NORMAL_IP)){
            // 正常记录+1
            context.getCounter(LogCounter.NORMAL).increment(1L);
            return true;
        }else{
            // 不正常记录+1
            context.getCounter(LogCounter.ABNORMAL).increment(1L);
            return false;
        }
    }


}
