package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.ThreadFactory;

/**
 * Hadoop hdfs java api 操作
 */
public class HDFSapp{
    public static final String HDFS_PATH = "hdfs://hadoop000:9000";


    FileSystem fileSystem = null;
    Configuration configuration = null;

    /**
     * 创建HDFS 目录
     */
    @Test
    public  void mkdir() throws  Exception{
       fileSystem.mkdirs(new Path("/hdfsapi/text"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception{
       FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
       output.write("hello hadoop".getBytes());
       output.flush();
       output.close();
    }

    /**
     *查看HDFS文件内容
     */
    @Test
    public void cat() throws Exception{
       FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath,newPath);
    }


    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public  void copyFromLocalFile() throws Exception{
        Path localPath = new Path("E:\\hmh\\zj.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);

    }

    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public  void copyFromLocalFileWithProgress() throws Exception{
        Path localPath = new Path("E:\\hmh\\hadoop-3.3.0.tar.gz");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);

        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("E:\\hmh\\hadoop-3.3.0.tar.gz")));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/hadoop-3.3.0.tar.gz"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print("."); //带进度提醒信息
                    }
                });
        IOUtils.copyBytes(in, output,4096);
    }

    /**
     * 下载HDFS文件
     */
    @Test
    public void copyToLocalFile() throws Exception{
        Path localPath = new Path("E:\\hmh\\");
        Path hdsfPath = new Path("/hdfsapi/test/hadoop-3.3.0.tar.gz");
        fileSystem.copyToLocalFile(hdsfPath, localPath);


    }


    /**
     * 查看某个目录下所有文件
     */
    @Test
    public void listFile() throws Exception{

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/input"));
        for(FileStatus fileStatus : fileStatuses){
            String isDir = fileStatus.isDirectory() ?"文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir+"\t"+replication+"\t"+len+"\t"+path);
        }
    }


    /**
     * 删除
     */
    @Test
    public  void delete() throws Exception {
        fileSystem.delete(new Path("/hdfsapi/test"),true);

    }

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setup");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");

    }
    @After
    public void tearDown() throws Exception {
    }

}