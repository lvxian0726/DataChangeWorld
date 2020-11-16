package customSink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.Jedis;
import scala.Tuple4;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RedisSink extends RichSinkFunction<Tuple4<String, String, String, String>> {


    private static   Jedis jedis = null;

    @Override
    public void invoke(Tuple4<String, String, String, String> value, Context context) throws Exception {

    jedis.hsetnx(value._1(),value._2(),value._3());

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("192.168.70.128", 6379);
        jedis.select(0);
    }

    @Override
    public void close() throws Exception {
        jedis.close();
        super.close();
    }
}
