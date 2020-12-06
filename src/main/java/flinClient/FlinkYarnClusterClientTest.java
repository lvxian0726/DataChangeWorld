package flinClient;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.net.URL;
import java.util.*;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_ADDRESS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_ADDRESS;

public class FlinkYarnClusterClientTest {


    private static final int PARALLELISM = 8;



    public static void main(String[] args) throws Exception {

        System.out.println("FlinkStandaloneClientTest-------------->start");


        //flink的本地配置目录，为了得到flink的配置
        String configurationDirectory = "/home/hadoop/flink-1.10.0/conf/";
//        String configurationDirectory = "E:\\IDEA\\git\\StreamingProjectForFlinkOrSpark\\src\\main\\resources\\flinkConf";
        //存放flink集群相关的jar包目录
        String flinkLibs = "hdfs://flink-1.10.0-data/data/flink/libs";
        //用户jar
        String userJarPath = "hdfs://flink-1.10.0-data/data/flink/user-lib/StreamingProjectForFlinkOrSpark-1.0-SNAPSHOT.jar";

        String flinkDistJar = "/home/hadoop/flink-1.10.0/data/flink-yarn_2.11-1.10.0.jar";

        Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(configurationDirectory);

        flinkConfiguration.set(PipelineOptions.JARS, Collections.singletonList(userJarPath));

        flinkConfiguration.set(YarnConfigOptions.VCORES, 4);

        flinkConfiguration.set(YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR, "FIRST");

        flinkConfiguration.set(YarnConfigOptions.FLINK_DIST_JAR, flinkDistJar);

//        flinkConfiguration.set(DeploymentOptions.TARGET, "yarn-per-job");

        flinkConfiguration.set(DeploymentOptions.ATTACHED, false);


        flinkConfiguration.set(YarnConfigOptions.APPLICATION_NAME, "first_submit_flink_job");

        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set(RM_ADDRESS,"192.168.70.132");

        YarnClient yarnClient = YarnClientImpl.createYarnClient();

        yarnClient.init(yarnConfiguration);

        yarnClient.start();

        int i = yarnClient.getNodeReports(NodeState.RUNNING)
                .stream()
                .mapToInt(report -> report.getCapability().getVirtualCores())
                .max()
                .orElse(0);

//        YarnJobClusterEntrypoint yarnJobClusterEntrypoint = new YarnJobClusterEntrypoint(flinkConfiguration);
//
//        yarnJobClusterEntrypoint.startCluster();

        YarnClientYarnClusterInformationRetriever yarnClientYarnClusterInformationRetriever = YarnClientYarnClusterInformationRetriever.create(yarnClient);


        int maxVcores = yarnClientYarnClusterInformationRetriever.getMaxVcores();

        System.out.println("maxVcores---------------->:" + maxVcores);

        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(flinkConfiguration, yarnConfiguration, yarnClient, yarnClientYarnClusterInformationRetriever, true);

        yarnClusterDescriptor.setLocalJarPath(new Path("/home/hadoop/StreamingProjectForFlinkOrSpark-1.0-SNAPSHOT.jar"));

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();



        List<URL> urls = new ArrayList<>();
        urls.add(new URL("file:///root/StormAndKafka-1.0-SNAPSHOT.jar"));

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


        String yarnClusterId = clusterClient.getClusterId().toString();

        System.out.println("yarnClusterId--------------->" + yarnClusterId);


        System.out.println("FlinkStandaloneClientTest-------------->end");


    }
}
