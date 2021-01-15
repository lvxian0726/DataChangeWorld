package com.lvxian.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class customGenerateSQLJobGraph_v1_10 {


    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {

        Configuration configuration = new Configuration();
        OptimizerPlanEnvironment planEnvironment = new OptimizerPlanEnvironment(configuration);

        ExecutionEnvironmentFactory factory = () -> {
            return planEnvironment;
        };

        Method method = ExecutionEnvironment.class.getDeclaredMethod("initializeContextEnvironment", ExecutionEnvironmentFactory.class);
        method.setAccessible(true);
        method.invoke((Object) null, factory);

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironmentFactory streamFactory = () -> {
            return streamExecutionEnvironment;
        };
        Method m1 = StreamExecutionEnvironment.class.getDeclaredMethod("initializeContextEnvironment", StreamExecutionEnvironmentFactory.class);

        m1.setAccessible(true);
        m1.invoke((Object) null, streamFactory);

        System.out.println("ok");


        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment,environmentSettings);


        String sql = " create table tbl1(\n" +
                "    a int,\n" +
                "    b bigint,\n" +
                "    c varchar\n" +
                "  ) with (\n" +
                "    'connector.type' = 'filesystem',\n" +
                "    'format.type' = 'csv',\n" +
                "    'connector.path' = 'xxx'\n" +
                "  )";

        String sql2 = " create table tbl2(\n" +
                "    a int,\n" +
                "    b bigint,\n" +
                "    c varchar\n" +
                "  ) with (\n" +
                "    'connector.type' = 'filesystem',\n" +
                "    'format.type' = 'csv',\n" +
                "    'connector.path' = 'xxx'\n" +
                "  )";

        String sql3 = "insert into tbl2" +
                "       select a,b,c from tbl1";



        streamTableEnvironment.sqlUpdate(sql);
        streamTableEnvironment.sqlUpdate(sql2);
        streamTableEnvironment.sqlUpdate(sql3);

        try {
            streamTableEnvironment.execute("first job name");
        } catch (Throwable e) {
            System.out.println("i`m error");

            Field field = OptimizerPlanEnvironment.class.getDeclaredField("pipeline");
            field.setAccessible(true);
            Pipeline pipeLine = (Pipeline) field.get(planEnvironment);
            if (pipeLine == null) {
                System.out.println("The program caused an error: pipeLine");
            }

            if (pipeLine instanceof StreamGraph) {
                JobGraph jobGraph = ((StreamGraph) pipeLine).getJobGraph();

                System.out.println(jobGraph.toString());
            }
            e.printStackTrace();
        }

    }


}
