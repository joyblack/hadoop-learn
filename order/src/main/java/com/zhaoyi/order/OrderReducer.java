package com.zhaoyi.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer< OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

//        System.out.println("------------");
//        System.out.print(key);
//        int count = 0;
//        for (NullWritable value:values) {
//            count++;
//        }
//        System.out.println("拥有"+ count + "条数据.");
//        System.out.print(key);
        context.write(key, NullWritable.get());
    }
}
