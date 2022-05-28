package com.dsp.dsp_assignment_2;

import com.dsp.aws.emr.AddSteps;
import com.dsp.aws.emr.CreateCluster;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

public class App {
    public static void main(String[] args) {
        EmrClient emr = EmrClient.builder().build();
        AddSteps.addNewStep(emr, "j-1SCPQTQPDY1U9",
                "s3://dsp-assignment-2/myWordCount.jar",
                "com.dsp.mr_app.step1WordCount",
                new String[]{},
                "wc3"
        );

//        CreateCluster.createAppClusterWithStep(emr,
//                "s3://dsp-assignment-2/myWordCount.jar",
//                "com.dsp.mr_app.myWordCount",
//                "linux_laptop",
//                "s3://dsp-assignment-2/logs",
//                "myWordCount"
//        );
        System.exit(0);
        HadoopJarStepConfig hadoopJarStepConfig = HadoopJarStepConfig.builder()
                .jar("s3://dsp-assignment-2/myWordCount.jar")
                .mainClass("com.dsp.mr_app.step1WordCount")
                .args("s3://dsp-assignment-2/input/data")
                .build();

        StepConfig config = StepConfig
                .builder()
                .name("my word count")
                .actionOnFailure(ActionOnFailure.CANCEL_AND_WAIT)
                .hadoopJarStep(hadoopJarStepConfig)
                .build();

        String jobFlowId = CreateCluster.createAppClusterWithStep(emr, "s3://dsp-assignment-2/myWordCount.jar",
                "com.dsp.mr_app.myWordCount",
                "linux-laptop",
                "s3://dsp-assignment-2/logs",
                "myWordCount");

    }
}
