package com.tedu.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Download {

    public static void main(String[] args) throws  Throwable{
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.65.161:9000");

        System.setProperty("HADOOP_USER_NAME","root");

        FileSystem fileSystem=FileSystem.get(configuration);

        Path hdfsPath=new Path("/item6/test1.txt");
        Path localPath=new Path("d:\\temp");
        boolean delSrc=false;
        //windows的文件系统安全有自己的特点
        boolean useRawLocalFileSystem=true;
        fileSystem.copyToLocalFile(delSrc,hdfsPath,localPath,useRawLocalFileSystem);

        fileSystem.close();
    }
}
