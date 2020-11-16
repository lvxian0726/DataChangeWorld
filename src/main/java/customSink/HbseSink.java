package customSink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HbseSink extends RichSinkFunction<String> {


    private static Connection connection = null;

    @Override
    public void invoke(String value, Context context) throws Exception {

        // Connection conn = HbaseUtil.getHBaseConn();+
        TableName tableName = TableName.valueOf("cdc_table");

        Table Htable = connection.getTable(tableName);
        long ttl = 600L;

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        JSONObject jsonObject = JSONObject.parseObject(value);
        String type = jsonObject.getString("type");
        JSONArray datas = jsonObject.getJSONArray("data");
        String database = jsonObject.getString("database");
        String table = jsonObject.getString("table");
        Long event_time = jsonObject.getLong("ts");
        String hbase_rowkey_eventtime = event_time.toString();
        Date date = new Date(event_time);
        String date_time = format.format(date);
        for (Object tmp : datas) {
            JSONObject json_data = JSONObject.parseObject(tmp.toString());
            String id = json_data.getString("id");
            //设置表名
            System.out.println(event_time + "|||" + tmp.toString());
            try {
                //设定行键rowkey
                Put put = new Put(Bytes.toBytes(hbase_rowkey_eventtime));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("event_time"), Bytes.toBytes(hbase_rowkey_eventtime));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date_time"), Bytes.toBytes(date_time));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("db_name"), Bytes.toBytes(database));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("table_name"), Bytes.toBytes(table));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("primary_key"), Bytes.toBytes(id));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("params"), Bytes.toBytes(tmp.toString()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("action_type"), Bytes.toBytes(type));
                put.setTTL(ttl);
                Htable.put(put);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                Htable.close();
            }


        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "192.168.70.132:60020");
        configuration.set("hbase.zookeeper.quorum", "192.168.70.130,192.168.70.132");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
