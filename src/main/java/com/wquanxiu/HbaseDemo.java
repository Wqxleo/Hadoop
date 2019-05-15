package com.wquanxiu;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wangquanxiu at 2019/4/16 19:59
 */
public class HbaseDemo {

    private static Admin admin;

    public static void main(String[] args){
        try {
            createTable("student", new String[] { "information", "scores" });
            Map<String,String> score1 = new HashMap<String, String>();
            score1.put("Chinese","89");
            score1.put("Math","92");
            score1.put("English","87");
            Student student = new Student("001", "xiaoming", "123456", "man", "20", score1, "1232821@csdn.com");
            insertData("student", student);
            Map<String,String> score2 = new HashMap<String, String>();
            score2.put("Chinese","87");
            score2.put("English","77");
            Student student2 = new Student("002", "xiaohong", "654321", "female", "18", score2, "214214@csdn.com");
            insertData("student", student2);
            List<Student> list = getAllData("student");
            System.out.println("--------------------插入两条数据后--------------------");
            for (Student student3 : list){
                System.out.println(student3.toString());
            }
            System.out.println("--------------------获取原始数据-----------------------");
            getNoDealData("student");
            System.out.println("--------------------根据rowKey查询--------------------");
            Student student4 = getDataByRowKey("student", "student-001");
            System.out.println(student4.toString());
            System.out.println("--------------------获取指定单条数据-------------------");
            String user_phone = getCellData("student", "student-001", "scores", "phone");
            System.out.println(user_phone);
            Map<String,String> score3 = new HashMap<String, String>();
            score3.put("Chinese","67");
            score3.put("Math","86");
//            score3.put("English","87");
            Student student5 = new Student("test-003", "xiaoguang", "789012", "man", "22", score3, "856832@csdn.com");
            insertData("student", student5);
            List<Student> list2 = getAllData("student");
            System.out.println("--------------------插入测试数据后--------------------");
            for (Student student6 : list2){
                System.out.println(student6.toString());
            }
            deleteByRowKey("student", "student-test-003");
            List<Student> list3 = getAllData("student");
            System.out.println("--------------------删除测试数据后--------------------");
            for (Student student7 : list3){
                System.out.println(student7.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //连接集群
    public static Connection initHbase() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.112.128"); //zookeeper IP地址
        config.set("hbase.zookeeper.property.clientPort", "2181"); //zookeeper端口
        config.set("hbase.master.hostname","ubuntu");  //服务端主机名
        Connection connection = ConnectionFactory.createConnection(config);
        return connection;
    }

    //创建表
    public static void createTable(String tableNmae, String[] cols) throws IOException {

        TableName tableName = TableName.valueOf(tableNmae);//设置表名
        admin = initHbase().getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("表已存在！");
        } else {
            //建立表的描述，添加表名
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            //添加列族
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            //创建表
            admin.createTable(hTableDescriptor);
        }
    }

    //插入数据
    public static void insertData(String tableName, Student student) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        Put put = new Put(("student-" + student.getId()).getBytes());
        //参数：1.列族名  2.列名  3.值
        put.addColumn("information".getBytes(), "name".getBytes(), student.getname().getBytes()) ;
        put.addColumn("information".getBytes(), "birthday".getBytes(), student.getbirthday().getBytes()) ;
        put.addColumn("information".getBytes(), "gender".getBytes(), student.getGender().getBytes()) ;
        put.addColumn("information".getBytes(), "classin".getBytes(), student.getclassin().getBytes()) ;
        put.addColumn("information".getBytes(), "address".getBytes(), student.getaddress().getBytes()) ;
        Map<String,String> scores = student.getscores();
        for(Map.Entry<String,String> entry: scores.entrySet()){
            put.addColumn("scores".getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
        }
        //HTable table = new HTable(initHbase().getConfiguration(),tablename);已弃用
        Table table = initHbase().getTable(tablename);
        table.put(put);
    }

    //获取原始数据
    public static void getNoDealData(String tableName){
        try {
            Table table= initHbase().getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resutScanner = table.getScanner(scan);
            for(Result result: resutScanner){
                System.out.println("scan:  " + result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //根据rowKey进行查询
    public static Student getDataByRowKey(String tableName, String rowKey) throws IOException {

        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Student student = new Student();
        student.setId(rowKey);
        //先判断是否有此条数据
        if(!get.isCheckExistenceOnly()){
            Result result = table.get(get);
            Map<String,String> scores = new HashMap<String, String>();
            for (Cell cell : result.rawCells()){
                String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                if(colName.equals("name")){
                    student.setname(value);
                }
                if(colName.equals("classin")){
                    student.setclassin(value);
                }
                if(colName.equals("birthday")){
                    student.setbirthday(value);
                }
                if (colName.equals("gender")){
                    student.setGender(value);
                }
                if (colName.equals("Chinese")||colName.equals("Math")||colName.equals("English")){
                    scores.put(colName,value);
                }
                if (colName.equals("address")){
                    student.setaddress(value);
                }
            }
            student.setscores(scores);
        }
        return student;
    }

    //查询指定单cell内容
    public static String getCellData(String tableName, String rowKey, String family, String col){

        try {
            Table table = initHbase().getTable(TableName.valueOf(tableName));
            String result = null;
            Get get = new Get(rowKey.getBytes());
            if(!get.isCheckExistenceOnly()){
                get.addColumn(Bytes.toBytes(family),Bytes.toBytes(col));
                Result res = table.get(get);
                byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
                return result = Bytes.toString(resByte);
            }else{
                return result = "查询结果不存在";
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "出现异常";
    }

    //查询指定表名中所有的数据
    public static List<Student> getAllData(String tableName){

        Table table = null;
        List<Student> list = new ArrayList<Student>();
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            ResultScanner results = table.getScanner(new Scan());
            Student student = null;
            for (Result result : results){
                String id = new String(result.getRow());
                System.out.println("用户名:" + new String(result.getRow()));
                student = new Student();
                Map<String,String> scores = new HashMap<String, String>();
                for(Cell cell : result.rawCells()){
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    //String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    student.setId(row);
                    if(colName.equals("name")){
                        student.setname(value);
                    }
                    if(colName.equals("classin")){
                        student.setclassin(value);
                    }
                    if(colName.equals("birthday")){
                        student.setbirthday(value);
                    }
                    if (colName.equals("gender")){
                        student.setGender(value);
                    }
                    if (colName.equals("Chinese")||colName.equals("Math")||colName.equals("English")){
                        scores.put(colName,value);
                    }
                    if (colName.equals("address")){
                        student.setaddress(value);
                    }
                }
                student.setscores(scores);
                list.add(student);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    //删除指定cell数据
    public static void deleteByRowKey(String tableName, String rowKey) throws IOException {

        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除指定列
        //delete.addColumns(Bytes.toBytes("scores"), Bytes.toBytes("address"));
        table.delete(delete);
    }

    //删除表
    public static void deleteTable(String tableName){

        try {
            TableName tablename = TableName.valueOf(tableName);
            admin = initHbase().getAdmin();
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}