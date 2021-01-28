package cn.layfolk.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author 王津
 * @Date 2020/7/17
 * @Version 1.0
 */
public class HDFSClient {


    //hdfs上创建文件夹
    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        //configuration.set("fs.defaultFS","hadfs://hadoop102:9000");
        //FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration,"atguigu");
        //2.创建目录
        fs.mkdirs(new Path("/test1/create"));
        //3.关闭资源
        fs.close();
    }

    //上传本地文件到hdfs
    @Test
    public void put() throws IOException, URISyntaxException, InterruptedException {
        //获取一个HDFS抽象封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration,"atguigu");
        //用这个对象操作文件系统
        fileSystem.copyFromLocalFile(new Path("D:\\预览文件夹\\004.jpg"), new Path("/"));
        //关闭文件系统
        fileSystem.close();
    }

    //HDFS文件下载到本地
    @Test
    public void get() throws IOException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration,"atguigu");
        //2.下载文件到本地
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fileSystem.copyToLocalFile(new Path("/004.jpg"), new Path("D:/"));
        //3.关闭资源
        fileSystem.close();
    }

    //hdfs文件名更改
    @Test
    public void rename() throws IOException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration,"atguigu");
        //2.修改名字
        fileSystem.rename(new Path("/004.jpg"), new Path("/005.jpg"));
        //3.关闭资源
        fileSystem.close();
    }

    //hdfs删除文件
    @Test
    public void testDelete() throws IOException, InterruptedException {
        //1.获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration,"atguigu");
        //2.删除文件
        fileSystem.delete(new Path("/1.txt"), true);
        //3.关闭资源
        fileSystem.close();
    }

    //hdfs文件详情查看
    @Test
    public void testListFiles() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem  = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration,"atguigu");
        //获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            System.out.println("============================");
            //输出详情
            //文件名称
            System.out.println(status.getPath().getName());
            //长度
            System.out.println(status.getLen());
            //权限
            System.out.println(status.getPermission());
            //分组
            System.out.println(status.getGroup());
            System.out.println("块信息：");
            //获取存储块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {

                //获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.print(host+"  ");
                }
            }

        }
        //关闭资源
        fileSystem.close();
    }

    //HDFS文件和文件夹判断
    @Test
    public void testListStatus() throws IOException, InterruptedException {
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"), configuration, "atguigu");

        //2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus) {
            //如果是文件
            if (status.isFile()){
                System.out.println("f:"+status.getPath().getName());
            }else {
                System.out.println("d:"+status.getPath().getName());
            }
        }
        //3.关闭资源
        fs.close();
    }


    //IO流操作，文件上传
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("D:/1.txt"));
        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/1.txt"));
        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }


    // 文件下载
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/1.txt"));
        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("d:/1.txt"));
        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    //读取超过128M的文件，分多次读取，合并
    //第一块
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));
        // 4 流的拷贝
        byte[] buf = new byte[1024];
        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }
        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }
    //第二块
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        // 3 定位输入数据位置
        fis.seek(1024*1024*128);
        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));
        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);
        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
    /*在Window命令窗口中进入到目录E:\，然后执行如下命令，对数据进行合并
    type hadoop-2.7.2.tar.gz.part2 >> hadoop-2.7.2.tar.gz.part1
    合并完成后，将hadoop-2.7.2.tar.gz.part1重新命名为hadoop-2.7.2.tar.gz。解压发现该tar包非常完整。*/



}
