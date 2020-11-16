package flink_with_kafka;


import com.alibaba.fastjson.JSONObject;
import customSink.HbseSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import java.util.Properties;

/**
 * https://www/jianshu.com/p/512476524d7d    delay dataStream
 * https://blog.csdn.net/shenshouniu/article/details/84453692 state explain
 * https://www.jianshu.com/p/e9a330399b30 state explain
 */
public class kafka2Hbase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        env.setParallelism(1);

        //设置Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置CheckPoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        //Hbase表名
        String hbase_table = "cdc_table";
        //TTL
        long ttl = 600L;

        //自定义的source
        Properties properties = new Properties();
        //配置服务器地址
        properties.put("bootstrap.servers", "192.168.70.128:9092");
        //配置消费者组
        properties.put("group.id", "lvxian1");
        //配置是否自动确认offset
        properties.put("enable.auto.commit", "false");
        //指定序列化
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<String> consumer011 = new FlinkKafkaConsumer<>("mysqlBinLogCanal", new SimpleStringSchema(), properties);

        consumer011.setStartFromEarliest();

        DataStreamSource<String> flinkTopic = env.addSource(consumer011);


        // 测试阶段1，只过滤出db_telecom.wordcount表的insert内容
        SingleOutputStreamOperator<String> filter_insert = flinkTopic.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String data) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(data);
                String type = jsonObject.getString("type");
                String database = jsonObject.getString("database");
                String table = jsonObject.getString("table");
                if (type.equals("INSERT") && database.equals("db_telecom") && table.equals("wordcount")) {
                    return true;
                }
                return false;
            }
        });

        filter_insert.print();

        filter_insert.addSink(new HbseSink());

        env.execute();
    }

}
