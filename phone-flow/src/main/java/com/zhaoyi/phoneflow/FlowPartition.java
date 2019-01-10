package com.zhaoyi.phoneflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartition extends Partitioner<Text, FlowBean> {
    public static final String PARTITION_136 = "136";
    public static final String PARTITION_137 = "137";
    public static final String PARTITION_138 = "138";
    public static final String PARTITION_139 = "139";
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        // default partition is 0.
        int partition = 0;

        // get the phone left 3 number.
        String phonePrefix = text.toString().substring(0,3);

        // get partition.
        if(PARTITION_136.equals(phonePrefix)){
            partition = 1;
        }else if(PARTITION_137.equals(phonePrefix)){
            partition = 2;
        }else if(PARTITION_138.equals(phonePrefix)) {
            partition = 3;
        }else if(PARTITION_139.equals(phonePrefix)) {
            partition = 4;
        }
        return partition;
    }
}
