package com.zhaoyi.flowbeansort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable, WritableComparable<FlowBean> {

    private long upFlow;// 上行流量
    private long downFlow;// 下行流量
    private long totalFlow;// 总流量

    // 无参构造
    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow + downFlow;
    }

    // desc sort.
    public int compareTo(FlowBean other) {
        return this.getTotalFlow()>other.getTotalFlow()? -1:1;
    }

    // 序列化
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(totalFlow);
    }

    // set方法，一次性设置属性
    public void set(long upFlow, long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.totalFlow = upFlow + downFlow;
    }

    // 反序列化 - 顺序和序列化保持一致
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.totalFlow = in.readLong();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getTotalFlow() {
        return totalFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    // 使用制表符分隔
    @Override
    public String toString() {
        return upFlow +
                "\t" + downFlow +
                "\t" + totalFlow;
    }
}
