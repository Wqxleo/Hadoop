package com.wquanxiu;

/**
 * Created by wangquanxiu at 2019/3/29 19:18
 */
import java.io.IOException;
import java.util.Vector;

import com.sun.corba.se.impl.orb.ParserTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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


public class StudentAverage {

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

        //将用户设置成服务器上可以使用hadoop的用户，避免权限问题
        System.setProperty("HADOOP_USER_NAME","wqx");
        @SuppressWarnings("deprecation")
        //生成新的job，名字为StudetAverage
        Job job = new Job(new Configuration(), "StudentAverage");
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
        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.112.128:9000/user/wquanxiu/grade"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.112.128:9000/user/wquanxiu/grade_output"));
        job.waitForCompletion(true);
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

            System.out.println(fileName+" "+string);
            context.write(new Text(fileName), new Text(string));
        };
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        Vector<String> MaxNames  = new Vector<String>();
        int MaxScore = 0;
        Vector<String> MinNames  = new Vector<String>();
        int MinScore = 100;
        int i = 0;

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            i++;
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

            if(max>MaxScore){
                MaxScore = max;
                MaxNames = highGrade;
            }
            else if(max == MaxScore){
                for(String name: highGrade){
                    MaxNames.add(name);
                }
            }

            if(min<MinScore){
                MinScore = min;
                MinNames = lowGrade;
            }
            else if(min == MinScore){
                for(String name: lowGrade){
                    MinNames.add(name);
                }
            }

            String avg = "Average score is "+ String.valueOf(sum / j)+"\n";

            String maxScore= "\nMax score is "+String.valueOf(max)+ ", studet(s) is(are)";
            for(String element: highGrade){
                maxScore += (" " + element);
            }
            maxScore+="\n";

            String minScore= "Min score is "+String.valueOf(min)+ ", studet(s) is(are)";
            for(String element: lowGrade){
                minScore += (" " + element);
            }
            minScore+="\n";

            String rank = "The rank : "+ "A is "+String.valueOf(Y)+", B is "+String.valueOf(L)+", C is "+String.valueOf(Z)+", D is "+String.valueOf(J)+", E is "+String.valueOf(B)+".\n";

            String result = maxScore+minScore+avg+rank;


            context.write(new Text(key), new Text(result));


            if(i == 4){

                String MaxGrade = "\nThe max score is "+String.valueOf(MaxScore)+",students are ";
                for(String name: MaxNames){
                    MaxGrade += (" "+name);
                }

                String MinGrade = "The min score is "+String.valueOf(MinScore)+",students are ";
                for(String name: MinNames){
                    MinGrade += (" "+name);
                }


                context.write(new Text("total:"), new Text(MaxGrade+"\n"+MinGrade));
            }
        };
    }
}