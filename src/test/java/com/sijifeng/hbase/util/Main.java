package com.sijifeng.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yingchun on 2018/9/26.
 */
public class Main {

    public static void main (String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = null;
        try {
            configuration.set("hbase.zookeeper.quorum", "192.168.0.22:2181");
            connection = ConnectionFactory.createConnection(configuration);
            demoCreateTable(connection);
//            demoPutDataInTable(connection);
//            demoUpdateDataInTable(connection);
//            demoScanTable(connection);
//            demoDeleteTableRow(connection);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void demoDeleteTableRow (Connection connection) {
        Table table = null;
        try {
            TableName tableName = TableName.valueOf("Member");
            table = connection.getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes("row1"));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void demoScanTable (Connection connection) {
        Table table = null;
        ResultScanner scanner = null;
        try {
            TableName tableName = TableName.valueOf("Member");
            table = connection.getTable(tableName);
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Name"));
            scan.addColumn(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Password"));
            RegexStringComparator comparator = new RegexStringComparator("Shark"); //
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Name"), CompareFilter.CompareOp.EQUAL, comparator);
            scan.setFilter(filter);
            scanner = table.getScanner(scan);
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                System.out.println("Result: " + result);
                System.out.println("Name: " + new String(result.getValue(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Name"))));
                System.out.println("Password: " + new String(result.getValue(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Password"))));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private static void demoUpdateDataInTable (Connection connection) {
        Table table = null;
        try {
            TableName tableName = TableName.valueOf("Member");
            table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Name"), Bytes.toBytes("CrazyShark"));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void demoPutDataInTable (Connection connection) {
        Table table = null;
        try {
            TableName tableName = TableName.valueOf("Member");
            table = connection.getTable(tableName);
            Put put1 = new Put(Bytes.toBytes("row1"));
            put1.addColumn(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Name"), Bytes.toBytes("Shark"));
            put1.addColumn(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Password"), Bytes.toBytes("SharkPassword"));
            table.put(put1);
            Put put2 = new Put(Bytes.toBytes("row2"));
            put2.addColumn(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Name"), Bytes.toBytes("SharkTest"));
            put2.addColumn(Bytes.toBytes("AccountInfo"), Bytes.toBytes("Password"), Bytes.toBytes("SharkTestPassword"));
            table.put(put2);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void demoCreateTable (Connection connection) {
        Admin admin = null;
        try {
            TableName tableName = TableName.valueOf("Member");
            admin = connection.getAdmin();
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            tableDescriptorBuilder
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("AccountInfo")).build());
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
