package com.zhaoyi.table.combine;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BookBean implements Writable {
    private String bookId;
    private String bookName;
    private String orderId;
    // 订单中对应书籍的购买数量
    private Integer buyNum;
    // 标记数据来源 book or order?
    private String beanSource;

    public BookBean() {
    }

    public BookBean(String bookId, String bookName, String orderId, Integer buyNum, String beanSource) {
        this.bookId = bookId;
        this.bookName = bookName;
        this.orderId = orderId;
        this.buyNum = buyNum;
        this.beanSource = beanSource;
    }

    @Override
    public String toString() {
        return orderId + '\t' + bookName + '\t' + buyNum;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(bookId);
        out.writeUTF(bookName);
        out.writeUTF(orderId);
        out.writeInt(buyNum);
        out.writeUTF(beanSource);
    }

    public void readFields(DataInput in) throws IOException {
        this.bookId = in.readUTF();
        this.bookName = in.readUTF();
        this.orderId = in.readUTF();
        this.buyNum = in.readInt();
        this.beanSource = in.readUTF();
    }

    public String getBookId() {
        return bookId;
    }

    public void setBookId(String bookId) {
        this.bookId = bookId;
    }

    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Integer getBuyNum() {
        return buyNum;
    }

    public void setBuyNum(Integer buyNum) {
        this.buyNum = buyNum;
    }

    public String getBeanSource() {
        return beanSource;
    }

    public void setBeanSource(String beanSource) {
        this.beanSource = beanSource;
    }
}
