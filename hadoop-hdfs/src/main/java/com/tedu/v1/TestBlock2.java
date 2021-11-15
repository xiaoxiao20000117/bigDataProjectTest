package com.tedu.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestBlock2 {
    static  byte[] byteArray=new byte[1024*1024];

    public static void main(String[] args) throws Throwable {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.65.161:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fileSystem = FileSystem.get(configuration);

        Path filePath = new Path("/test6.txt");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);

        for (int i=0;i<135;i++){
        fsDataOutputStream.write(byteArray);
           System.out.println("i="+i);
       }
        fsDataOutputStream.close();
    }
}
