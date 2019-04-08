package com.wquanxiu.Utils;

/**
 * Created by wangquanxiu at 2019/4/8 15:17
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSUtil {
    private FileSystem fs;
    public HDFSUtil(String user){
        Configuration cfg = new Configuration();
        URI uri = null;
        try {
            uri = new URI("hdfs://192.168.112.128:9000");
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        try {
            // 根据配置文件，实例化成DistributedFileSystem
            fs = FileSystem.get(uri, cfg, user); // 得到fs句柄
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传文件
     */
    public void upload(String src, String dst){
        try {
            // 上传
            fs.copyFromLocalFile(new Path(src), new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载文件
     */
    public void download(String src, String dst){
        try {
            // 下载
            fs.copyToLocalFile(new Path(src), new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件夹
     */
    public void mkdir(String dir){
        try {
            fs.mkdirs(new Path(dir));
        } catch (IOException e) {
            // 创建目录
            e.printStackTrace();
        }
    }

    /**
     * 删除文件
     */
    public void delete(String name, boolean recursive){
        try {
            fs.delete(new Path(name), recursive);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 重命名
     */
    public void rename(String source, String dst){
        try {
            fs.rename(new Path(source), new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列出文件信息
     */
    public void list(String dir, boolean recursive){
        try {
            RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(dir), recursive);
            while (iter.hasNext()){
                LocatedFileStatus file = iter.next();
                System.out.println(file.getPath().getName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}