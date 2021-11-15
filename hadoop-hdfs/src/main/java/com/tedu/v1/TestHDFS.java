package com.tedu.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;

public class TestHDFS {
    public static void main(String[] args) throws Throwable {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.65.161:9000");
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsPath = new Path("/item6");
        if (fileSystem.exists(hdfsPath) == false) {
            fileSystem.mkdirs(hdfsPath);
        }

        Path srcPath = new Path("d:\\test1.txt");
        fileSystem.copyFromLocalFile(srcPath, hdfsPath);

        hdfsPath=new Path("/item6/test1.txt");
        Path localPath = new Path("d:\\temp");
        fileSystem.copyToLocalFile(false,hdfsPath, localPath,true);

        fileSystem.close();
    }
}
