package flinClient;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;


public class FlinkYarnClusterClientTest {


    private static final int PARALLELISM = 8;


    public static void main(String[] args) throws Exception {

        System.out.println("FlinkStandaloneClientTest-------------->start");


        //flink的本地配置目录，为了得到flink的配置
        String configurationDirectory = "/home/hadoop/flink-1.10.0/conf/";
        //存放flink集群相关的jar包目录
        String flinkLibs = "hdfs://flink-1.10.0-data/data/flink/libs";

        String flinkDistJar = "/home/hadoop/flink-1.10.0/lib/flink-yarn_2.11-1.10.0.jar";

        Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(configurationDirectory);

        flinkConfiguration.set(YarnConfigOptions.VCORES, 1);
        flinkConfiguration.set(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, "/home/hadoop/flink-1.10.0/conf/log4j.properties");
//        flinkConfiguration.set(YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR, "FIRST");

        ArrayList<String> list = new ArrayList<>();

        list.add("/home/hadoop/flink-1.10.0/lib/flink-yarn_2.11-1.10.0.jar");
        list.add("/home/hadoop/flink-1.10.0/lib/flink-runtime-web_2.11-1.10.1.jar");
        list.add("/home/hadoop/flink-1.10.0/lib/slf4j-log4j12-1.7.15.jar");
        list.add("/home/hadoop/flink-1.10.0/lib/log4j-1.2.17.jar");
        list.add("/home/hadoop/flink-1.10.0/conf/log4j.properties");

        flinkConfiguration.set(YarnConfigOptions.SHIP_DIRECTORIES, list);

        flinkConfiguration.set(SavepointConfigOptions.SAVEPOINT_PATH, "hdfs:///flink-1.10.0-savepoints");
        flinkConfiguration.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);

        flinkConfiguration.set(YarnConfigOptions.FLINK_DIST_JAR, flinkDistJar);

//        flinkConfiguration.set(DeploymentOptions.TARGET, "yarn-per-job");

        flinkConfiguration.set(DeploymentOptions.ATTACHED, false);


        flinkConfiguration.set(YarnConfigOptions.APPLICATION_NAME, "first_submit_flink_job");

        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        YarnClient yarnClient = YarnClientImpl.createYarnClient();

        yarnClient.init(yarnConfiguration);

        yarnClient.start();

        YarnClientYarnClusterInformationRetriever yarnClientYarnClusterInformationRetriever = YarnClientYarnClusterInformationRetriever.create(yarnClient);

        int maxVcores = yarnClientYarnClusterInformationRetriever.getMaxVcores();

        System.out.println("maxVcores---------------->:" + maxVcores);

        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(flinkConfiguration, yarnConfiguration, yarnClient, yarnClientYarnClusterInformationRetriever, true);


        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();


        List<URL> urls = new ArrayList<>();

        String jar = "/home/hadoop/StreamingProjectForFlinkOrSpark-1.0-SNAPSHOT.jar";

        String[] arg = new String[]{"name", "lvxian"};
        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(new File(jar))
                .setArguments(arg)
                .setEntryPointClassName("com.lvxian.streaming.flinkEventTimeStatusTest")
                .setUserClassPaths(urls)
                .build();


        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, flinkConfiguration, PARALLELISM, false);

        ClusterClientProvider<ApplicationId> applicationIdClusterClientProvider = yarnClusterDescriptor.deployJobCluster(clusterSpecification, jobGraph, true);


        ClusterClient<ApplicationId> clusterClient = applicationIdClusterClientProvider.getClusterClient();

        JobID flinkJobID = jobGraph.getJobID();

        String yarnClusterId = clusterClient.getClusterId().toString();

        /**
         *  2020-12-13 23:47
         * 此方法重要，可以从当前flink on yarn任务中 ，凭借applicationid取回cluster flink client对象句柄，已进行后续的client层级的API逻辑处理
         */
//        ClusterClientProvider<ApplicationId> clusterClientProvider = yarnClusterDescriptor.retrieve(clusterClient.getClusterId());


        System.out.println("savepoint path:" +  flinkConfiguration.getString(SavepointConfigOptions.SAVEPOINT_PATH));


        long ts = System.currentTimeMillis();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");

        while (System.currentTimeMillis() - ts < 5 * 60 * 1000) {
            Thread.sleep(20000);
            System.out.println("当前时间--------------->：" + simpleDateFormat.format(new Date()));
            System.out.println("当前flink job: " + flinkJobID.toString() + " 状态：" + clusterClient.getJobStatus(flinkJobID).thenAccept(value -> System.out.println(value)));
            String applicationStatus = yarnClient.getApplicationReport(clusterClient.getClusterId()).getYarnApplicationState().name();
            System.out.println("当前yarn application job: " + yarnClusterId + " 状态：" + applicationStatus);
            CompletableFuture<String> triggerSavepoint = clusterClient.triggerSavepoint(flinkJobID, flinkConfiguration.getString(SavepointConfigOptions.SAVEPOINT_PATH));  //触发savepoint方法
            triggerSavepoint.thenAccept(value -> System.out.println("触发savepoint成功，返回路径---------->" + value));
        }
        System.out.println("FlinkStandaloneClientTest-------------->end");
    }
}
