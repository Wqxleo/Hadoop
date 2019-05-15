package com.wquanxiu;

/**
 * Created by wangquanxiu at 2019/3/29 19:18
 */
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

import javax.security.sasl.SaslServer;

import static org.apache.hadoop.hdfs.client.HdfsUtils.*;


public class StudentAverage {

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

        //将用户设置成服务器上可以使用hadoop的用户，避免权限问题
        System.setProperty("HADOOP_USER_NAME","wqx");
        Configuration conf = new Configuration();
        @SuppressWarnings("deprecation")
        //生成新的job，名字为StudetAverage
        Job job = new Job(conf, "StudentAverage");
        Job job2 = new Job(conf,"Total");

        String inputPath1 = "hdfs://192.168.112.128:9000/user/wquanxiu/grade";
        String outputPath1 = "hdfs://192.168.112.128:9000/user/wquanxiu/grade_output";

        String inputPath2 = "hdfs://192.168.112.128:9000/user/wquanxiu/grade_output";
        String outputPath2 = "hdfs://192.168.112.128:9000/user/wquanxiu/grade_output_total";

        HDFSUtil hdfsUtil = new HDFSUtil("wqx");

        hdfsUtil.delete(outputPath1,true);
        hdfsUtil.delete(outputPath2,true);

        //设置主类
        job.setJarByClass(StudentAverage.class);
        //设置Mapper类
        job.setMapperClass(Map.class);
        //设置Reducer类
        job.setReducerClass(Reduce.class);
        //设置Map输出时Key的类型，在这里我们的Key是文件名，所以设置为Text类
        job.setMapOutputKeyClass(Text.class);
        //设置Map输出时value的类型，在这里我们的valeu是姓名和成绩组成的字符串，所以设置为Text类
        job.setMapOutputValueClass(Text.class);
        //设置输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出文件路径
        FileInputFormat.setInputPaths(job, new Path(inputPath1));
        FileOutputFormat.setOutputPath(job, new Path(outputPath1));


        //设置主类
        job2.setJarByClass(StudentAverage.class);
        //设置Mapper类
        job2.setMapperClass(TotalMap.class);
        //设置Reducer类
        job2.setReducerClass(TotalReduce.class);
        //设置Map输出时Key的类型，在这里我们的Key是文件名，所以设置为Text类
        job2.setMapOutputKeyClass(Text.class);
        //设置Map输出时value的类型，在这里我们的valeu是姓名和成绩组成的字符串，所以设置为Text类
        job2.setMapOutputValueClass(Text.class);
        //设置输出的key和value的类型
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        //设置输入输出文件路径
        FileInputFormat.setInputPaths(job2, new Path(inputPath2));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath2));
        if(job.waitForCompletion(true)){
            job2.waitForCompletion(true);
        }



        System.out.println("运行结束！");
    }

    public static class Map extends Mapper<Object, Text, Text, Text>{
        protected void map(Object key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            //获取当前的文件名
            InputSplit inputSplit = context.getInputSplit();
            String fileName = ((FileSplit) inputSplit).getPath().toString();

            String string= new String(value.getBytes(),0,value.getLength(),"UTF-8");

//            String data = value.toString();

//            System.out.println(fileName+" "+string);
            context.write(new Text(fileName), new Text(string));
        };
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {


        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Vector<String> highGrade = new Vector<String>();
            Vector<String> lowGrade = new Vector<String>();
            int Y = 0;
            int L = 0;
            int Z = 0;
            int J = 0;
            int B = 0;
            int max = 0;
            int min = 100;
            int j = 0;
            for (Text value : values) {
                j++;
                String[] information = value.toString().split(" ");
                String name = information[0];
                int grade = Integer.valueOf(information[1]);
                //计算总分
                sum += grade;
                //计算最高分，并记录姓名
                if(grade>max){
                    max = grade;
                    highGrade.clear();
                    highGrade.add(name);
                }
                else if(grade == max){
                    highGrade.add(name);
                }

                //计算最低分，并记录姓名
                if(grade<min){
                    min = grade;
                    lowGrade.clear();
                    lowGrade.add(name);
                }
                else if(grade == min){
                    lowGrade.add(name);
                }
                //成绩分等级
                if(grade >= 90){
                    Y++;
                }
                else if(grade>= 80){
                    L++;
                }
                else if(grade>= 70){
                    Z++;
                }
                else if(grade >= 60){
                    J++;
                }
                else {
                    B++;
                }
            }
            String avg = "Average score is "+ String.valueOf(sum / j)+"\n";
            String maxScore= "\nMaxscore: "+String.valueOf(max)+ " studets:";
            for(String element: highGrade){
                maxScore += (" " + element);
            }
            maxScore+="\n";

            String minScore= "Minscore: "+String.valueOf(min)+ " studets:";
            for(String element: lowGrade){
                minScore += (" " + element);
            }
            minScore+="\n";

            String rank = "The rank : "+ "A is "+String.valueOf(Y)+", B is "+String.valueOf(L)+", C is "+String.valueOf(Z)+", D is "+String.valueOf(J)+", E is "+String.valueOf(B)+".\n";
            String result = maxScore+minScore+avg+rank;
            context.write(new Text(key), new Text(result));
        };
    }

    public static class TotalMap extends Mapper<Object, Text, Text, Text>{
        protected void map(Object key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            String string= new String(value.getBytes(),0,value.getLength(),"UTF-8");
//            System.out.println("Map: "+string);
            String[] strings = string.split(" ");
//            String data = value.toString();
//            System.out.println(strings[0]);
            if(strings[0].equals("Maxscore:") || strings[0].equals("Minscore:")){
                System.out.println("Map: "+string);
                context.write(new Text(strings[0]), new Text(string));
            }

        };
    }


    public static class TotalReduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Vector<String> MaxNames  = new Vector<String>();
            int MaxScore = 0;
            Vector<String> MinNames  = new Vector<String>();
            int MinScore = 100;

            System.out.println(key.toString());
            if(key.toString().equals("Maxscore:")){
                for (Text value : values) {
                    String[] strings = value.toString().split(" ");

                    int n = strings.length;
                    int grade = Integer.valueOf(strings[1]);
                    if(grade > MaxScore){
                        MaxScore = grade;
                        MaxNames.clear();
                        for (int i = 3; i < n;i++){
                            MaxNames.add(strings[i]);
                        }
                    }
                    else if(grade == MaxScore){
                        for (int i = 3; i < n;i++){
                            MaxNames.add(strings[i]);
                        }
                    }
                }

                StringBuilder max = new StringBuilder(key + " " + MaxScore+" students:");
                for(String name: MaxNames){
                    max.append(" ").append(name);
                }
                context.write(new Text("total "), new Text(max.toString()));
            }

            if(key.toString().equals("Minscore:")){
                for (Text value : values) {
                    String[] strings = value.toString().split(" ");
                    int n = strings.length;
                    int grade = Integer.valueOf(strings[1]);
                    if(grade < MinScore){
                        MinScore = grade;
                        MinNames.clear();
                        MinNames.addAll(Arrays.asList(strings).subList(3, n));
                    }
                    else if(grade == MaxScore){
                        for (int i = 3; i < n;i++){
                            MinNames.add(strings[i]);
                        }
                    }
                }
                String min = key+" "+MinScore+" students:";
                for(String name: MinNames){
                    min += (" " +name);
                }
                context.write(new Text("total "), new Text(min));
            }

        }
    }

}