package com.tedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestBlock1 {
    static byte[] data=new byte[1024*1024];
    public static void main(String[] args) throws  Throwable{
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.65.161:9000");

        System.setProperty("HADOOP_USER_NAME","root");

        FileSystem fileSystem=FileSystem.get(configuration);

        Path hdfsPath=new Path("/item6/test1.txt");
        Path destPath = new Path("d:\\temp");
fileSystem.copyToLocalFile(hdfsPath,destPath);
        fileSystem.close();


    }
}
