package freewheel.rpt;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;

public class HBaseMain {
    public static Configuration configuration;
    private static String tableName = "wal-test3";
    private static HTable hTable;
    static {
        configuration = HBaseConfiguration.create();
        try {
            hTable = new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        //String tb ="wal-t2";
         createTable(tableName);
        for (int i = 0; i < 2000; i++) {
            insertData(i);
        }
        for (int i = 0; i <50 ; i++) {
            deleteRow(i);
        }
        //insertData("wujintao");
        // QueryAll("wujintao");
        // QueryByCondition1("wujintao");
        // QueryByCondition2("wujintao");
        //QueryByCondition3("wujintao");
        //deleteRow("wujintao","abcdef");
        //deleteByCondition("wujintao","abcdef");
    }

    /**
     * 创建表
     * @param tableName
     */
    public static void createTable(String tableName) {
        System.out.println("start create table ......");
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
                System.out.println(tableName + " is exist,detele....");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
            tableDescriptor.addFamily(new HColumnDescriptor("cf2"));
            tableDescriptor.addFamily(new HColumnDescriptor("cf3"));
            hBaseAdmin.createTable(tableDescriptor);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end create table ......");
    }

    public static void insertData(int index) throws IOException{
        System.out.println("start insert data ......");


        Put put = new Put(("rowkey1"+index).getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.add("cf1".getBytes(), "c".getBytes(), ("aaa"+index).getBytes());// 本行数据的第一列
        put.add("cf2".getBytes(), "c".getBytes(), ("bbb"+index).getBytes());// 本行数据的第三列
        put.add("cf3".getBytes(), "c".getBytes(), ("ccc"+index).getBytes());// 本行数据的第三列
        put.setWriteToWAL(true);
        try {
            hTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end insert data ......");
    }

    /**
     * 删除一张表
     * @param tableName
     */
    public static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(configuration);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void deleteRow(int index)  {
        try {
            Delete d1 = new Delete(("rowkey"+index).getBytes());

            d1.setWriteToWAL(true);
            hTable.delete(d1);
            System.out.println("删除行成功!");

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * 组合条件删除
     * @param tablename
     * @param rowkey
     */
    public static void deleteByCondition(String tablename, String rowkey)  {
        //目前还没有发现有效的API能够实现 根据非rowkey的条件删除 这个功能能，还有清空表全部数据的API操作

    }


    /**
     * 查询所有数据
     * @param tableName
     */
    public static void QueryAll(String tableName) {
        HTablePool pool = new HTablePool(configuration, 1000);
        HTable table = (HTable) pool.getTable(tableName);
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
