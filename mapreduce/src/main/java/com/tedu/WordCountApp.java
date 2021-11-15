package com.tedu;

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
    public static void main(String[] args) throws  Throwable{
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS","hdfs://127.0.0.1:9000");

        System.setProperty("HADOOP_USER_NAME","zjj");

        Job job=Job.getInstance(configuration,"wordCount");
        job.setJarByClass(WordCountApp.class);

        FileInputFormat.setInputPaths(job,new Path("/words/words.txt"));
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job,new Path("/words/result5.txt"));
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);

    }

    static class WordCountMap extends Mapper<LongWritable, Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            System.out.println("key="+key+",value="+value);
            String line=value.toString();
            String[] words=line.split("\\ ");
            for (String word:words){
                context.write(new Text(word),new LongWritable(1));
            }
        }
    }

    static  class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
           // super.reduce(key, values, context);
            long count=0;
            for (LongWritable wordCount:values){
                count=count+wordCount.get();
                System.out.println("reduce key="+key+",value="+wordCount);
            }
            context.write(key,new LongWritable(count));
        }
    }
}
