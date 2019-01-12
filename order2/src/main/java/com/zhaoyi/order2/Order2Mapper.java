package com.zhaoyi.order2;

import com.sun.javafx.binding.StringFormatter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

public class Order2Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String,String> books = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        Files.lines(Paths.get(context.getCacheFiles()[0])).forEach(
                s -> {
                    String[] strings = s.split("\t");
                    // bookId and bookName
                    books.put(strings[0].trim(),strings[1]);
                }
        );
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 接下来只需拼接书的名字到最后就可以了
        String line = value.toString();
        String[] params = line.split("\t");
        // 获取书籍ID
        String bookId = params[1];

        // 获取当前对应的书籍名称
        String bookName = books.get(bookId);

        System.out.println(bookId == "1001");
        System.out.println(bookName);

        // 输出  orderId bookName buyNum
        context.write(new Text(String.format("%s\t%s\t%s", params[0], bookName, params[2])),NullWritable.get());
    }
}
