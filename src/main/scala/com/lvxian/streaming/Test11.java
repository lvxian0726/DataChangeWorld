package com.lvxian.streaming;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Test11 {


    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {

        Configuration configuration = new Configuration();
        OptimizerPlanEnvironment planEnvironment = new OptimizerPlanEnvironment(new Optimizer(configuration));

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


        Method m2 = StreamExecutionEnvironment.class.getDeclaredMethod("initializeContextEnvironment", StreamExecutionEnvironmentFactory.class);

        m2.setAccessible(true);
        m2.invoke((Object) null, streamFactory);

        System.out.println("ok");


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        SocketTextStreamFunction socketTextStreamFunction = new SocketTextStreamFunction("192.168.70.1", 12345, "\n", 5);

        DataStreamSource<String> dataStreamSource = environment.addSource(socketTextStreamFunction);

        dataStreamSource.addSink(new PrintSinkFunction<String>());

        try {
            environment.execute();
        } catch (Throwable e) {
            System.out.println("i`m error");

            Field field = OptimizerPlanEnvironment.class.getDeclaredField("optimizerPlan");
            field.setAccessible(true);
            FlinkPlan flinkPlan = (FlinkPlan) field.get(planEnvironment);
            if (flinkPlan == null) {
                System.out.println("The program caused an error: flinkPlan");
            }

            if (flinkPlan instanceof StreamGraph) {
                JobGraph jobGraph = ((StreamGraph) flinkPlan).getJobGraph();

                System.out.println(jobGraph.toString());
            }
            e.printStackTrace();
        }

    }


}
