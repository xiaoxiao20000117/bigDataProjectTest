package com.tedu.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateDirectory {
    public static void main(String[] args) throws  Throwable {
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.65.161:9000");

        System.setProperty("HADOOP_USER_NAME","root");

        FileSystem fileSystem=FileSystem.get(configuration);
        Path path=new Path("/item6");
        if (fileSystem.exists(path)==false){
            fileSystem.mkdirs(path);
        }
        fileSystem.close();
    }
}
