package com.zhaoyi.flowbeansort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean bean = new FlowBean();
    Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] strings = line.split("\t");
        // Key-FlowBean: get the up and downFlow.
        bean.set(Long.parseLong(strings[1]), Long.parseLong(strings[2]));
        // Val-Phone
        phone.set(value);
        context.write(bean, phone);
    }
}
