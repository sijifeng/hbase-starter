package com.sijifeng.hbase.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yingchun on 2018/9/26.
 */
public class HBaseUtil {
    private static Configuration conf = null;
    private static Connection con = null;
    private static Admin admin = null;

    static {
        // 获得配制文件对象
        conf = HBaseConfiguration.create();
        // 设置配置参数
        conf.set("hbase.zookeeper.quorum", "192.168.0.22");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //conf.set("zookeeper.znode.parent", "/hbase");
    }

    public synchronized static Connection getConnection () {
        try {
            if (null == con || con.isClosed()) {
                // 获得连接对象
                con = ConnectionFactory.createConnection(conf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return con;
    }

    public static void close () {
        try {
            if (admin != null) {
                admin.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void creatTable (String tableName, String[] columnFamily) {
        if (null == tableName || tableName.length() == 0) {
            return;
        }
        if (null == columnFamily || columnFamily.length == 0) {
            return;
        }
        TableName tn = TableName.valueOf(tableName);
        try {
            admin = getConnection().getAdmin();
            if (admin.tableExists(tn)) {
                System.out.println(tableName + " table exists,delete it.");
                admin.disableTable(tn);
                admin.deleteTable(tn);
                System.out.println("deleted.....");
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tn);
            for (String str : columnFamily) {
                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str)).build());
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println(tableName + " created!");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void insert (String tableName, String rowKey, String family, String qualifier, String value) {
        Table table = null;
        try {
            table = getConnection().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            System.out.println(tableName + " update fail!");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void insertBatch (String tableName, List<?> list) {
        if (null == tableName || tableName.length() == 0) {
            return;
        }
        if (null == list || list.size() == 0) {
            return;
        }
        Table t = null;
        Put put = null;
        JSONObject json = null;
        List<Put> puts = new ArrayList<>();
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            for (int i = 0, j = list.size(); i < j; i++) {
                json = (JSONObject) list.get(i);
                put = new Put(Bytes.toBytes(json.getString("rowKey")));
                put.addColumn(Bytes.toBytes(json.getString("family")), Bytes.toBytes(json.getString("qualifier")),
                        Bytes.toBytes(json.getString("value")));
                puts.add(put);
            }
            t.put(puts);
            System.out.println(tableName + " update done!");
        } catch (IOException e) {
            System.out.println(tableName + " update failed!");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void delete (String tableName, String rowKey) {
        delete(tableName, rowKey, "", "");
    }

    public static void delete (String tableName, String rowKey, String family) {
        delete(tableName, rowKey, family, "");
    }

    public static void delete (String tableName, String rowKey, String family, String qualifier) {
        if (null == tableName || tableName.length() == 0) {
            return;
        }
        if (null == rowKey || rowKey.length() == 0) {
            return;
        }
        Table t = null;
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            if (null != family && family.length() > 0) {
                if (null != qualifier && qualifier.length() > 0) {
                    del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                } else {
                    del.addFamily(Bytes.toBytes(family));
                }
            }
            t.delete(del);
        } catch (IOException e) {
            System.out.println("!");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void select (String tableName) {
        if (null == tableName || tableName.length() == 0) {
            return;
        }
        Table t = null;
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            // 读取操作
            Scan scan = new Scan();
            ResultScanner rs = t.getScanner(scan);
            if (null == rs) {
                return;
            }
            for (Result result : rs) {
                List<Cell> cs = result.listCells();
                if (null == cs || cs.size() == 0) {
                    continue;
                }
                for (Cell cell : cs) {
                    Map<String, Object> map = new HashMap<String, Object>();
                    map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// 取行健
                    map.put("timestamp", cell.getTimestamp());// 取到时间戳
                    map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// 取到列族
                    map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// 取到列
                    map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// 取到值
                    list.add(map);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    public static void select (String tableName, String rowKey) {
        select(tableName, rowKey, "", "");
    }

    public static void select (String tableName, String rowKey, String family) {
        select(tableName, rowKey, family, "");
    }

    public static void select (String tableName, String rowKey, String family, String qualifier) {
        Table t = null;
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            if (null != family && family.length() > 0) {
                if (null != qualifier && qualifier.length() > 0) {
                    get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
                } else {
                    get.addFamily(Bytes.toBytes(family));
                }
            }
            Result r = t.get(get);
            List<Cell> cs = r.listCells();
            if (null == cs || cs.size() == 0) {
                return;
            }
            for (Cell cell : cs) {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// 取行健
                map.put("timestamp", cell.getTimestamp());// 取到时间戳
                map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// 取到列族
                map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// 取到列
                map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// 取到值
                list.add(map);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }
}

