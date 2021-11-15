package com.tedu.tf01;

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
import java.security.Key;

public class WordCountApp {
    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://127.0.0.1:9000");
        System.setProperty("HADOOP_USER_NAME","ccx2000117");
        Job job = Job.getInstance(configuration,"wordCount");
        job.setJarByClass(WordCountApp.class);
        FileInputFormat.addInputPaths(job,"/words/words.txt");
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Key.class);
        FileOutputFormat.setOutputPath(job,new Path("/words/result02.txt"));
        job.setReducerClass(WordCountReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.waitForCompletion(true);

    }
    static class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            System.out.println("key="+key+",value="+value);
            String line = value.toString();
            String[] words = line.split("\\ ");
            for (String word:words){
                context.write(new Text(word),new LongWritable(1));
            }
        }
    }
    static class  WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //super.reduce(key, values, context);
            System.out.println("key="+key);
            long count = 0;
            for (LongWritable wordCount:values){
                count = count+wordCount.get();
            }
            context.write(new Text(key),new LongWritable(count));
            System.out.println("key="+key+",出现的次数="+count);
        }
    }
}
