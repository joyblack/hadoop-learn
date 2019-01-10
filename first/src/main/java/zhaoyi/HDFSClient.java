package zhaoyi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.net.URI;

public class HDFSClient {
    private final static String HADOOP_USER = "root";
    private final static String HDFS_URL = "hdfs://h133:8020";

    public static void main(String[] args) throws Exception {
        // 1、获取配置实体
        final Configuration configuration = new Configuration();
        // 2、获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL),configuration,HADOOP_USER);
        // 3、拷贝本地数据到集群
        fileSystem.copyFromLocalFile(new Path("D:/hadoop/test.txt"),new Path("/user/zhaoyi/input"));
        // 4、关闭文件系统
        fileSystem.close();
    }

    @Test
    // 获取文件系统
    public void getFileSystem() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL),configuration,HADOOP_USER);
        // print fs info
        System.out.println(fileSystem);
        fileSystem.close();
    }

    @Test
    public void putFileToHDFS() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER);
        // set destination path
        String destinationPath = "/user/yuanyong";
        // uploadFile
        fileSystem.copyFromLocalFile(true, new Path("D:/hadoop/前尘如梦.txt"),new Path(destinationPath) );
        // close fs
        fileSystem.close();
    }

    @Test
    public void getFileFromHDFS() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER);
        // set download path.
        String downloadPath = "D:/";
//        fileSystem.copyToLocalFile(new Path("/user/new.file"),new Path(downloadPath));
        fileSystem.copyToLocalFile(false,new Path("/user/new.file"),new Path(downloadPath),false);
        fileSystem.close();
    }

    @Test
    public void makeDirInHDFS() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER);
        fileSystem.mkdirs(new Path("/user/areya/my/best/love"));
        System.out.println("create dir success.");
        fileSystem.close();
    }

    @Test
    public void deleteInHDFS() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER);
        fileSystem.delete(new Path("/user/areya"),true);
        System.out.println("delete dir success.");
        fileSystem.close();
    }

    @Test
    public void renameInHDFS() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER);
        fileSystem.rename(new Path("/user/yuanyong"),new Path("/user/hongqun"));
        System.out.println("rename dir success.");
        fileSystem.close();
    }

    @Test
    public void getFileInfoInHDFS() throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER);

        // get all file.
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        while (listFiles.hasNext()){
            System.out.println("------------------");
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("path: " + fileStatus.getPath());
            System.out.println("group: " + fileStatus.getGroup());
            System.out.println("owner: " + fileStatus.getOwner());
            System.out.println("access time: " + fileStatus.getAccessTime());
            System.out.println("block size: " + fileStatus.getBlockSize());
            System.out.println("length: " + fileStatus.getLen());
            System.out.println("modify time: " + fileStatus.getModificationTime());
            System.out.println("modify time: " + fileStatus.getModificationTime());
            System.out.println("permission: " + fileStatus.getPermission());
            System.out.println("replicate: " + fileStatus.getReplication());
//            System.out.println("symlink: " + fileStatus.getSymlink());

            System.out.println("This file blocks info:");
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation:blockLocations) {
                System.out.println("block length:" + blockLocation.getLength());
                System.out.println("block offsets:" + blockLocation.getOffset());

                System.out.println("block hosts:");
                for (String host : blockLocation.getHosts()) {
                    System.out.println(host);
                }


            }

        }

        System.out.println("-------------");
        System.out.println("print end.");
    }

    @Test
    public void getAllFileInfoInHDFS() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HADOOP_USER);

        // get dir "/user"'s file.
        FileStatus[] listFiles = fileSystem.listStatus(new Path("/user"));
        for (FileStatus fileStatus:listFiles) {
            if(fileStatus.isDirectory()){
                System.out.println("dir:" + fileStatus.getPath().getName());
            }else if(fileStatus.isFile()){
                System.out.println("file:" + fileStatus.getPath().getName());
            }else if(fileStatus.isEncrypted()){
                System.out.println("cry:" + fileStatus.getPath().getName());
            }else if(fileStatus.isSymlink()){
                System.out.println("symlink:" + fileStatus.getPath().getName());
            }

        }

        System.out.println("-------------");
        System.out.println("print end.");
    }

    
}
