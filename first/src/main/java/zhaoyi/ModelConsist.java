package zhaoyi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;


// 一致性模型验证
public class ModelConsist {
    private final static String HADOOP_USER = "root";
    private final static String HDFS_URL = "hdfs://h133:8020";

    @Test
    public void testConsist() throws Exception {
        Path path = new Path("/user/consisit.txt");

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(HDFS_URL),configuration,HADOOP_USER);
        System.out.println(fs.getFileStatus(path).getLen());
        // 获取输出流
        FSDataOutputStream fos = fs.create(path);
        // 写入数据
        fos.write("some intersted text".getBytes());
        System.out.println(fs.getFileStatus(path).getLen());
        // 只对当前操作的节点刷新
        fos.hsync();
        fos.close();
    }
}
