package flink_with_kafka;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import entity.TableInfo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.Properties;

/**
 * https://www/jianshu.com/p/512476524d7d    delay dataStream
 * https://blog.csdn.net/shenshouniu/article/details/84453692 state explain
 * https://www.jianshu.com/p/e9a330399b30 state explain
 */
public class Flink_canal2mysql_from_kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);


        env.setParallelism(1);

        //设置Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置CheckPoint
            env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

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


//        flinkTopic.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
//
//            long maxTimestamp = 0L;
//            long maxOutOfOrder = 5L;
//
//            String pattern = "yyyy-MM-dd HH:mm:ss";
//            SimpleDateFormat format = new SimpleDateFormat(pattern);
//
//            @Nullable
//            @Override
//            public Watermark getCurrentWatermark() {
//                return new Watermark(maxTimestamp - maxOutOfOrder);
//            }
//
//            @Override
//            public long extractTimestamp(String data, long l) {
//                JSONObject jsonObject = JSONObject.parseObject(data);
//                Long event_time = jsonObject.getLong("ts");
//                maxTimestamp = event_time - maxTimestamp > 0 ? event_time : maxTimestamp;
//                return event_time;
//            }
//        });


        // 测试阶段，只过滤出db_telecom.wordcount表的insert内容
        SingleOutputStreamOperator<String> filter_insert = flinkTopic.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String data) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(data);

                String type = jsonObject.getString("type");
                String database = jsonObject.getString("database");
                String table = jsonObject.getString("table");
                BigInteger event_time = jsonObject.getBigInteger("ts");
                BigInteger es = jsonObject.getBigInteger("es");
                int id = jsonObject.getIntValue("id");
                boolean isDdl = jsonObject.getBooleanValue("isDdl");
                JSONObject mysqlType = jsonObject.getJSONObject("mysqlType");
                JSONArray old_data = jsonObject.getJSONArray("old");
                JSONArray pkNames = jsonObject.getJSONArray("pkNames");
                String sql = jsonObject.getString("sql");
                String sqlType = jsonObject.getString("sqlType");
                JSONArray array = jsonObject.getJSONArray("data");
                if (type.equals("INSERT") && database.equals("db_telecom") && table.equals("wordcount")) {
                    return true;
                }
                return false;
            }
        });


        SingleOutputStreamOperator<TableInfo> flatMap_insert = filter_insert.flatMap(new FlatMapFunction<String, TableInfo>() {
            @Override
            public void flatMap(String data, Collector<TableInfo> collector) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                JSONObject jsonObject = JSONObject.parseObject(data);
                String type = jsonObject.getString("type");
                JSONArray datas = jsonObject.getJSONArray("data");
                String database = jsonObject.getString("database");
                String table = jsonObject.getString("table");
                Long event_time = jsonObject.getLong("ts");
                Date date = new Date(event_time);
                String date_time = format.format(date);
                for (Object tmp : datas) {
                    JSONObject json_data = JSONObject.parseObject(tmp.toString());
                    String id = json_data.getString("id");
                    TableInfo tableInfo = new TableInfo(event_time.toString(), date_time, database, table, id, tmp.toString(), type);
                    collector.collect(tableInfo);
                }
            }
        });

        SingleOutputStreamOperator<String> flatMap_insert_hdfs = filter_insert.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String data, Collector<String> collector) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                JSONObject jsonObject = JSONObject.parseObject(data);
                String type = jsonObject.getString("type");
                JSONArray datas = jsonObject.getJSONArray("data");
                String database = jsonObject.getString("database");
                String table = jsonObject.getString("table");
                Long event_time = jsonObject.getLong("ts");
                Date date = new Date(event_time);
                String date_time = format.format(date);
                for (Object tmp : datas) {
                    JSONObject json_data = JSONObject.parseObject(tmp.toString());
                    String id = json_data.getString("id");
                    String concat_data = event_time.toString() + "\t" + date_time + "\t" +
                            database + "\t" + table + "\t" + id + "\t" + tmp.toString() + "\t" + type;
                    collector.collect(concat_data);
                }
            }
        });


        String[] fieldNames = new String[]{"event_time", "date_time", "db_name", "table_name", "primary_key", "params", "action_type"};
        DataType[] fieldTypes = new DataType[]{DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()};


        CsvTableSink csvTableSink = new CsvTableSink("C:\\Users\\Administrator\\Desktop\\flink_sink\\file", ",", 1, FileSystem.WriteMode.NO_OVERWRITE,
                fieldNames, fieldTypes);


        // 添加输出算子到hdfs文件系统
        // BucketingSink<String> sink = new BucketingSink<String>("C:\\Users\\Administrator\\Desktop\\flink_sink\\hdfs_sink");
        BucketingSink<String> sink = new BucketingSink<String>("hdfs://RedHatLv14:9000/hdfs_sink");

        sink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd_HH_mm", ZoneId.of("Asia/Shanghai")));
        sink.setBatchSize(1024 * 1024); // this is 1 MB,
        sink.setBatchRolloverInterval(60 * 1000); // this is 1 mins

        flatMap_insert_hdfs.addSink(sink);


//        streamTableEnvironment.registerTableSink("table_info", csvTableSink);
//
//        streamTableEnvironment.registerDataStream("ods_table_info", flatMap_insert);

        // streamTableEnvironment.registerTable("ods_table_info", fromDataStream);
        // fromDataStream.printSchema();
        // streamTableEnvironment.sqlUpdate("insert into table_info select event_time,db_name,table_name,primary_key,params,action_type from ods_table_info");


        // 添加输出算子到CSV注册的Table中
//        Table ods_source = streamTableEnvironment.scan("ods_table_info").select(" event_time,date_time,db_name,table_name,primary_key,params,action_type");
//
//        ods_source.printSchema();
//        ods_source.insertInto("table_info");


        env.execute();
    }

}
