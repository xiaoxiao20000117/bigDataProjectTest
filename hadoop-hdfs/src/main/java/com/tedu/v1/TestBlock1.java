package com.tedu.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;

public class TestBlock1 {
    public static void main(String[] args) throws Throwable {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.65.161:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fileSystem = FileSystem.get(configuration);

        Path filePath = new Path("/test2.txt");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
        fsDataOutputStream.writeBytes("111");
        fsDataOutputStream.close();
    }
}
