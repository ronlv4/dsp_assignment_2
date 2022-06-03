package com.dsp.dsp_assignment_2;

import com.dsp.aws.emr.AddSteps;
import com.dsp.aws.emr.CreateCluster;
import com.dsp.mr_app.step1BigramDecadeCount;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

public class App {
    public static final String BUCKET_HOME_SCHEME = "s3://dsp-assignment-2/";
    public static final Region REGION = Region.US_EAST_1;

    public static void main(String[] args) {
//        String language = args[1];
        EmrClient emr = EmrClient.builder().region(REGION).build();

        String jobFlowId = "j-ODR83P3DUR9H";
//        jobFlowId = CreateCluster.createCluster(emr,
//               "linux_laptop",
//                BUCKET_HOME_SCHEME + "logs",
//                3);
//        System.out.println("JobFlowId: " + jobFlowId);
        AddSteps.addNewStep(
                emr,
                jobFlowId,
                BUCKET_HOME_SCHEME + "myWordCount.jar",
                "com.dsp.dsp_assignment_2.TestSteps",
                new String[]{},
                "wc4"
        );

//        CreateCluster.createAppClusterWithStep(emr,
//                "s3://dsp-assignment-2/myWordCount.jar",
//                "com.dsp.mr_app.step1BigramDecadeCount",
//                "linux_laptop",
//                "s3://dsp-assignment-2/logs",
//                "myWordCount",
//                1
//        );
        System.exit(0);
        HadoopJarStepConfig hadoopJarStepConfig = HadoopJarStepConfig.builder()
                .jar(BUCKET_HOME_SCHEME + "myWordCount.jar")
                .mainClass("com.dsp.mr_app.step1WordCount")
                .args(BUCKET_HOME_SCHEME + "input/data")
                .build();

        StepConfig config = StepConfig
                .builder()
                .name("my word count")
                .actionOnFailure(ActionOnFailure.CANCEL_AND_WAIT)
                .hadoopJarStep(hadoopJarStepConfig)
                .build();

//        String jobFlowId = CreateCluster.createAppClusterWithStep(emr, "s3://dsp-assignment-2/myWordCount.jar",
//                "com.dsp.mr_app.myWordCount",
//                "linux-laptop",
//                "s3://dsp-assignment-2/logs",
//                "myWordCount");

    }
}
