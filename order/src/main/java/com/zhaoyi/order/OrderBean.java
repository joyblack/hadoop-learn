package com.zhaoyi.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;

    private double price;

    // second order
    public int compareTo(OrderBean o) {
        // according to order id.
        int result = this.orderId.compareTo(o.getOrderId());

        // then by price
        if(result == 0){
            result = this.getPrice() > o.getPrice()? -1:1;
        }

        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }

    public OrderBean() {
    }

    public void set(String orderId, double price){
        this.orderId = orderId;
        this.price = price;
    }

    public OrderBean(String orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price ;
    }
}
