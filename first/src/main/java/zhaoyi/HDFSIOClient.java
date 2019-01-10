package zhaoyi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSIOClient {
    private final static String HADOOP_USER = "root";
    private final static String HDFS_URL = "hdfs://h133:8020";

    @Test
    public void uploadFile() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL),configuration, HADOOP_USER);
        // 1. 获取输入流
        FileInputStream fis = new FileInputStream(new File("D:/hadoop/天净沙.txt"));
        // 2. 获取输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/user/天净沙.txt"));
        // 3. 流对接 & 关闭流
        try {
            IOUtils.copyBytes(fis, fos,configuration);
            System.out.println("upload file success.");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            fis.close();
            fos.close();
            fileSystem.close();
        }
    }

    @Test
    public void downloadFile() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL),configuration, HADOOP_USER);
        // 1. 获取输入流
        FSDataInputStream fis = fileSystem.open(new Path("/user/奥丁的子女.txt"));
        // 2. 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:/hadoop/奥丁的子女.txt"));

        // 3. 流对接&关闭流
        try {
            IOUtils.copyBytes(fis, fos,configuration);
            System.out.println("download file success.");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            fis.close();
            fos.close();
            fileSystem.close();
        }
    }

    @Test
    public void downloadFileBySeek1() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL),configuration, HADOOP_USER);
        // part 1. 128M
        // 1. 获取输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/user/hadoop-2.7.2.tar.gz"));
        // 2. 获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File("D:/hadoop/hadoop-2.7.2.tar.gz.part1"));
        // 3. 流对接(第一部分只需读取128M即可)
        byte[] buff = new byte[1024];
        try {
            for (int i = 0; i < 1024 * 128; i++) {
                inputStream.read(buff);
                outputStream.write(buff);
            }
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
            IOUtils.closeStream(fileSystem);
        }

    }

    @Test
    public void downloadFileBySeek2() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL),configuration, HADOOP_USER);
        // part 1. 128M
        // 1. 获取输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/user/hadoop-2.7.2.tar.gz"));
        // 2. 获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File("D:/hadoop/hadoop-2.7.2.tar.gz.part2"));
        // 3. 指向第二块数据地址
        inputStream.seek(1024 * 1024 * 128);
        try {
            IOUtils.copyBytes(inputStream,outputStream,configuration);
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
            IOUtils.closeStream(fileSystem);
        }

    }

    @Test
    public void testConsist() throws Exception {
        Path path = new Path("/user/consisit.txt");

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFS_URL),configuration,HADOOP_USER);
        System.out.println(fs.getFileStatus(path).getLen());


    }
    
}
