package flink_with_kafka;


import com.alibaba.fastjson.JSONObject;
import customSink.HbseSink;
import customSink.RedisSink;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import redis.clients.jedis.Jedis;
import scala.Tuple3;
import scala.Tuple4;

import java.util.Properties;

/**
 * https://www/jianshu.com/p/512476524d7d    delay dataStream
 * https://blog.csdn.net/shenshouniu/article/details/84453692 state explain
 * https://www.jianshu.com/p/e9a330399b30 state explain
 */
public class Socket2Redis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //设置Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //设置CheckPoint

        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        //Hbase表名

        SocketTextStreamFunction function = new SocketTextStreamFunction("192.168.70.1",1234,"\n",5);

        DataStreamSource<String> source = env.addSource(function);


        source.map(new RichMapFunction<String, String>() {

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
            }

            @Override
            public RuntimeContext getRuntimeContext() {
                return super.getRuntimeContext();
            }

            @Override
            public IterationRuntimeContext getIterationRuntimeContext() {
                return super.getIterationRuntimeContext();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public String map(String s) throws Exception {
                return null;
            }
        });

        // 测试阶段1，只过滤出db_telecom.wordcount表的insert内容
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> map = source.map(new MapFunction<String, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> map(String data) throws Exception {
                System.out.println("input data:" + data);
                JSONObject object = JSONObject.parseObject(data);
                String name = object.getString("name");
                String news_id = object.getString("news_id");
                String click = object.getString("click");
                String show = object.getString("show");

                return new Tuple4<>(name, news_id, click, show);
            }
        });

        map.print();

        map.addSink(new RedisSink());

        env.execute();
    }

}
