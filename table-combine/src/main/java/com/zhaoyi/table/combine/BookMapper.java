package com.zhaoyi.table.combine;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class BookMapper extends Mapper<LongWritable, Text, Text, BookBean> {
    BookBean bookBean = new BookBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] strings = line.split("\t");

        // 区分不同的文件来源
        FileSplit inputSplit = (FileSplit)context.getInputSplit();

        // 根据两个文件的不同区分处理
        if(inputSplit.getPath().getName().startsWith(BeanSource.PREFIX_BOOK_FILE)){
            bookBean.setBeanSource("book_index");
            bookBean.setBookId(strings[0]);
            bookBean.setBookName(strings[1]);
            bookBean.setBuyNum(0);
            bookBean.setOrderId("0");
        }else{
            bookBean.setBeanSource("book_order");
            bookBean.setOrderId(strings[0]);
            bookBean.setBookId(strings[1]);
            bookBean.setBuyNum(Integer.valueOf(strings[2]));
            bookBean.setBookName("");
        }
        // 以书籍ID作为排序
        context.write(new Text(bookBean.getBookId()), bookBean);
    }
}
