package com.zhaoyi.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupComparator extends WritableComparator {

    public OrderGroupComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aa = (OrderBean) a;
        OrderBean bb = (OrderBean) b;
        return aa.getOrderId().compareTo(bb.getOrderId());
    }
}
