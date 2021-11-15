package com.tedu.first;

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

public class WordCount3 {
    public static void main(String[] args) throws  Throwable{
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        System.setProperty("HADOOP_USER_NAME", "zjj");

        Job job=Job.getInstance(configuration,"wordCount");

        job.setJarByClass(WordCount3.class);

        //lib.input
        FileInputFormat.setInputPaths(job,"/words/words.txt");
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileOutputFormat.setOutputPath(job,new Path("/words/out13"));

        job.waitForCompletion(true);

    }

    static class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //不能有super(),有super()只执行一次
            System.out.println("key="+key+","+value);
            String line=value.toString();
            String[] words=line.split("\\ ");

            for (String word:words) {
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    static class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("reduce 汇总"+key+":"+values);
            long count=0;
            for (LongWritable v:values){
                count=count+v.get();
            }
            System.out.println("reduce 汇总"+key+":"+count);
context.write(key,new LongWritable(count));


        }
    }
}
