package com.tedu.second;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountApp {
    public static void main(String[] args) throws Throwable {
        //3，配置和创建任务
        //3.1配置hadoop服务器信息
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        //3.2配置用户名
        System.setProperty("HADOOP_USER_NAME", "ccx2000117");

        //3.3创建作业
        Job job = Job.getInstance(configuration, "wordCount");
        job.setJarByClass(WordCountApp.class);

        //3.4设置mapper相关信息
        //lib.input.FileInputFormat
        FileInputFormat.setInputPaths(job, "/words/words.txt");
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //3.5设置reduce配置
        FileOutputFormat.setOutputPath(job, new Path("/words/out3"));
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //3.6启动作业
        job.waitForCompletion(true);
    }
    //I love you
    //I love love you
    //I love love love you

    //1,任务分配
    //0,Text,Text,LongWritable
    //12 Text,Text,LongWritable
    //29 Text,Text,LongWritable
    static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        //key:每行偏移量0,12,29
        //value:文本
        //context:作业id,保存数据，给reduce用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            System.out.println("map key="+key+",value="+value);
            String line=value.toString();
            String[] words=line.split("\\ ");
            for (String word:words){
                //保存到hadoop框架中
                context.write(new Text(word),new LongWritable(1));
            }

        }
    }

    //2,任务汇总
    //进来的数据key的类型 Text
    //进来的数据value的类型 LongWritable
    //汇总结果数据key的类型 Text
    //汇总结果数据value的类型LongWritable
    static class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //必须注释掉
            //super.reduce(key, values, context);
            long count=0;
            //i : [1,1,1]
            //love:[1,1,1,1,1,1]
            for (LongWritable wordcount:values){
                count=count+wordcount.get();
            }
            //结果保存到hdfs中
            context.write(new Text(key),new LongWritable(count));
            System.out.println("reduce key="+key+",count="+count);
        }
    }
}
