package flinClient;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FlinkStandaloneClientTest {


    private static final int PARALLELISM = 8;
    private static final Configuration FLINK_CONFIG = new Configuration();


    public static void main(String[] args) throws Exception {

        System.out.println("FlinkStandaloneClientTest-------------->start");

        FLINK_CONFIG.setString(JobManagerOptions.ADDRESS, "192.168.70.130");
        FLINK_CONFIG.setInteger(JobManagerOptions.PORT, 6123);
        FLINK_CONFIG.setInteger(RestOptions.PORT, 8081);
        FLINK_CONFIG.setString(RestOptions.ADDRESS, "192.168.70.130");
        FLINK_CONFIG.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 3);


        RestClusterClient<StandaloneClusterId> flinkClient = new RestClusterClient<>(FLINK_CONFIG, StandaloneClusterId.getInstance());

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

        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, FLINK_CONFIG, PARALLELISM, false);


        JobID jobId = flinkClient.submitJob(jobGraph).get();


        String job_id_string = jobId.toHexString();

        System.out.println("job_id_string:" + job_id_string);

        CompletableFuture<JobStatus> jobStatus = flinkClient.getJobStatus(jobId);

        JobStatus status = jobStatus.get();

        System.out.println("job_status:" + status.name());

        System.out.println("WebInterfaceURL:" + flinkClient.getWebInterfaceURL());

        long ts = System.currentTimeMillis();

        System.out.println("loop start time:" + ts);
        while (System.currentTimeMillis() - ts < 1000 * 60 * 10) {
            Thread.sleep(5000);
            System.out.println("当前状态：" + flinkClient.getJobStatus(jobId).get().name());

        }

        System.out.println("FlinkStandaloneClientTest-------------->end");


    }
}
