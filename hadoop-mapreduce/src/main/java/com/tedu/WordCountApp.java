package com.tedu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountApp {
    //客户端代码，写完交给ResourceManager框架去执行
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://127.0.0.1:9000");

        System.setProperty("HADOOP_USER_NAME", "zjj");
        Job job = Job.getInstance(configuration, WordCountApp.class.getSimpleName());
        //打成jar执行
        job.setJarByClass(WordCountApp.class);

        //数据在哪里？
        FileInputFormat.setInputPaths(job, "/words/words.txt");
        //使用哪个mapper处理输入的数据？
        job.setMapperClass(MyMapper.class);
        //map输出的数据类型是什么？
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //使用哪个reducer处理输入的数据？
        job.setReducerClass(MyReducer.class);
        //reduce输出的数据类型是什么？
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //数据输出到哪里？
        FileOutputFormat.setOutputPath(job, new Path("/words/out5"));

        //交给yarn去执行，直到执行结束才退出本程序
        job.waitForCompletion(true);
    }
    //自定义的mapper，继承org.apache.hadoop.mapreduce.Mapper
    public static class MyMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
//split  函数是用于按指定字符（串）或正则去分割某个字符串，结果以字符串数组形式返回，这里按照"\t"来分割text文件中字符，即一个制表符，这就是为什么我在文本中用了空格分割，导致最后的结果有很大的出入。
            String[] splited = line.split(" ");
//foreach 就是 for（元素类型t 元素变量x:遍历对象obj）{引用x的java语句}
            for (String word : splited) {
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    public static class MyReducer extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s,
                              Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {

            long count = 0L;
            for (LongWritable v2 : v2s) {
                long wordcount=v2.get();
                count =count+ wordcount;
            }
            LongWritable v3 = new LongWritable(count);
            context.write(k2, v3);
        }
    }


}
