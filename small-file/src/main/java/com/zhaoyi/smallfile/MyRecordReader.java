package com.zhaoyi.smallfile;

import javafx.util.converter.DateStringConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private UUID uuid = UUID.randomUUID();
    Boolean more = true;// 是否还有更多的数据需要读
    BytesWritable value = new BytesWritable();// 输出值
    FileSplit split;// 分片信息
    private Configuration configuration;// 配置信息
    public MyRecordReader() {
        super();
    }


    // 初始化信息
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("uuid = " + uuid);
        System.out.println("########initialize#######");
        split = (FileSplit)inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        System.out.println("#######nextKeyValue###########");
        System.out.println(split.getPath());
        if(more){
            System.out.println("this is more ...");
            // 定义缓存
            byte[] contents = new byte[(int)split.getLength()];
            // 获取文件系统
            FileSystem fs  = FileSystem.get(this.configuration);
            // 读取文件内容
            FSDataInputStream fsDataInputStream = fs.open(split.getPath());
            // 读入缓冲区
            IOUtils.readFully(fsDataInputStream,contents,0,contents.length);
            // 输出到value
            value.set(contents, 0, contents.length);

            more = false;// 已经处理完
            return true; //返回true表示还会迭代该方法（下一次不会进入这里待处理逻辑，more），这在一次没处理完时很有用，虽然我们这里并无意义。
        }
        return  false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
