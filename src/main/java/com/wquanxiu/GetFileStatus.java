package com.wquanxiu;

import javax.security.auth.login.AppConfigurationEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * Created by wangquanxiu at 2019/3/27 9:01
 */
public class GetFileStatus {
    public static void main(String[] args)throws Exception{
        String url = "hdfs://192.168.112.128:9000/user/wquanxiu/input";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(url),conf);
        Path delPath = new Path(url);
        if(fs.exists(delPath)){
            FileStatus[] statuses = fs.listStatus(delPath);
            for(FileStatus status: statuses){
                System.out.println(status);
            }

        }
        else {
            System.out.println("Path not exits");
        }
    }
}
