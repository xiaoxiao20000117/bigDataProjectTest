package com.tedu.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UpLoad {
    public static void main(String[] args) throws  Throwable{
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.65.161:9000");

        System.setProperty("HADOOP_USER_NAME","root");

        FileSystem fileSystem=FileSystem.get(configuration);
        Path srcPath=new Path("d:\\test1.txt");
        Path hdfsPath=new Path("/item6");
        fileSystem.copyFromLocalFile(srcPath,hdfsPath);
        fileSystem.close();
    }
}
