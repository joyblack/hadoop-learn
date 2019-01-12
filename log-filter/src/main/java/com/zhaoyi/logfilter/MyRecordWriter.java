package com.zhaoyi.logfilter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.Charset;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream outputStreamInc = null;
    FSDataOutputStream outputStreamExc = null;
    public MyRecordWriter() {
        super();
    }

    public MyRecordWriter(TaskAttemptContext taskAttemptContext) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(taskAttemptContext.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 设置两个输出路径，分别用输入包含指定信息的文件内容以及其他的内容
        try {
            outputStreamInc = fs.create(new Path(LogFilterConfig.OUTPUTPATH_INC));
            outputStreamExc = fs.create(new Path(LogFilterConfig.OUTPUTPATH_EXC));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String line = new String(text.getBytes(),0,text.getLength(),"GBK");
        line += "\r\n";
        if(line.contains(LogFilterConfig.FILTER_STRING)){
            outputStreamInc.write(line.getBytes());
        }else{// put in exclude path.
            outputStreamExc.write(line.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            outputStreamInc.close();
            outputStreamExc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
