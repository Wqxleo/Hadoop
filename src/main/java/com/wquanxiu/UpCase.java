package com.wquanxiu;

import com.wquanxiu.Utils.HDFSUtil;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Vector;
import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

import com.sun.corba.se.impl.orb.ParserTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.wquanxiu.Utils.HDFSUtil;


/**
 * Created by wangquanxiu at 2019/4/22 16:52
 */

public class UpCase {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context)
                throws java.io.IOException, InterruptedException {


            String string = new String(value.getBytes(), 0, value.getLength(), "UTF-8");

            context.write(new Text("String"), new Text(string));
        }

        ;
    }

    public static class UpCaseReduce extends Reducer<Text, Text, Text, Text> {


        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int numchar = 0;
            int num = 0;
            for(Text value:values) {
                String string = value.toString();
                for(int i = 0; i < string.length();i++) {
                    if(Character.isUpperCase(string.charAt(i))||Character.isLowerCase(string.charAt(i))){
                        numchar++;
                    }
                    if(string.charAt(i)>='0' && string.charAt(i)<='9'){
                        num++;
                    }
                }
            }
            context.write(new Text("num"),new Text("char:"+String.valueOf(numchar)+"\nnum: "+String.valueOf(num)));

        }

        ;
    }

    public static class LongStringReduce extends Reducer<Text, Text, Text, Text> {


        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int longest = 0;
            Vector<String> longString = new Vector<String>();
            for(Text value:values) {
                String[] line = value.toString().split(" ");
                for(String string:line){
                    if(string.length()>longest){
                        longest = string.length();
                        longString.clear();
                        longString.add(string);
                    }
                    else if(string.length()==longest){
                        longString.add(string);
                    }
                }

            }
            String result = "";
            for(String string:longString){
                result+=(string+" ");
            }
            context.write(new Text("longString:"),new Text(result));

        }

        ;
    }




        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            System.setProperty("HADOOP_USER_NAME", "wqx");
            HDFSUtil hdfsUtil = new HDFSUtil("wqx");

//
//            String src = "file:///G:/Programming/linux/3.1.c";
//            String dst = "hdfs://192.168.112.128:9000/user/wquanxiu/upload/test.c";
//
//            hdfsUtil.upload(src,dst);



            String input = "hdfs://192.168.112.128:9000/user/wquanxiu/upcase/";
            String output = "hdfs://192.168.112.128:9000/user/wquanxiu/output_upcase";


            hdfsUtil.delete(output,true);

            Job job = new Job(conf, "word count");
            job.setJarByClass(UpCase.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(UpCaseReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

            }
}
