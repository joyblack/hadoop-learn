package com.zhaoyi.phoneflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private final int PHONE_NUMBER_INDEX = 1;
    private final int UP_FLOW_BACKWARDS = 3;
    private final int DOWN_FLOW_BACKWARDS = 2;
    private Text outKey = new Text();
    private FlowBean outVal = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 输入的key是id号，不需要处理
        // 1. 获取一行数据
        String line = value.toString();

        // 2.截取数据
        String[] strings = line.split("\t");

        // 3.获取key - 电话号码
        String phoneNumber = strings[PHONE_NUMBER_INDEX];
        outKey.set(phoneNumber);

        // 4.获取输出val - flowBean
        // （1）获取上行流量
        long upFlow = Long.parseLong(strings[strings.length - UP_FLOW_BACKWARDS]);
        // （2）获取下行流量
        long downFlow = Long.parseLong(strings[strings.length - DOWN_FLOW_BACKWARDS]);
        outVal.set(upFlow, downFlow);

        // 2.写数据
        context.write(outKey, outVal);
    }
}
