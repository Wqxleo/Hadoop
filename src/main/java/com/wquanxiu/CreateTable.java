package com.wquanxiu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.ArrayList;
import java.util.List;

public class CreateTable {
    private static Connection connection;
    private static void initConnectionInstance() throws Exception{
        if (connection == null) {
            synchronized (CreateTable.class) {
                if (connection == null) {
                    Configuration config = HBaseConfiguration.create();
                    config.set("hbase.zookeeper.quorum", "192.168.112.128");
                    config.set("hbase.zookeeper.property.clientPort", "2181");
                    config.set("hbase.master.hostname","ubuntu");
                    connection = ConnectionFactory.createConnection(config);
                }
            }
        }
    }

    public static void createTable(String tableNameStr, List<String> columnFamilies) throws Exception {
        if (tableNameStr == null || tableNameStr.equals("") || columnFamilies == null || columnFamilies.size() == 0) {
            throw new RuntimeException("parameter is empty or illegal");
        }
        if (connection == null) {
            initConnectionInstance();
        }
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            System.out.println(admin);
            TableName tableName = TableName.valueOf(tableNameStr);
            List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<ColumnFamilyDescriptor>(columnFamilies.size());
            for (String cf : columnFamilies) {
                familyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build());
            }
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamilies(familyDescriptors)
                    .build();
            if (admin.tableExists(tableName)) {
                return;
            }
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            throw e;
        } finally {
            admin.close();
        }
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","wqx");
        String tableNameStr = "student";
        List<String> columnFamilies = new ArrayList(){
            {
                add("testColumnFamily");
            }
        };
        createTable(tableNameStr,columnFamilies);
    }
}
