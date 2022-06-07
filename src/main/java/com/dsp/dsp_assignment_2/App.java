package com.dsp.dsp_assignment_2;

import com.dsp.aws.emr.ClusterOperations;
import com.dsp.aws.emr.StepsOperations;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.Arrays;

public class App {
    public static final String BUCKET_HOME_SCHEME = "s3://dsp-assignment-2/";
    public static final Region REGION = Region.US_EAST_1;
    public static final String KEYS = "linux_laptop";
    public static final int INSTANCE_COUNT = 3;

    public static void main(String[] args) {

        // START OF TESTING SECTION

//        String language = args[1];
        EmrClient emr = EmrClient.builder().region(REGION).build();

//        String jobFlowId = "j-13WCCUKJB25DU";
//        jobFlowId = CreateCluster.createCluster(emr,
//               key,
//                BUCKET_HOME_SCHEME + "logs",
//                3);
//        System.out.println("JobFlowId: " + jobFlowId);
//        AddSteps.addNewStep(
//                emr,
//                jobFlowId,
//                BUCKET_HOME_SCHEME + "myWordCount.jar",
//                "com.dsp.dsp_assignment_2.TestSteps",
//                new String[]{},
//                "wc5"
//        );


//        System.exit(0); // END OF TESTING SECTION
        // Real App:

        System.out.println("args" + Arrays.toString(args));

        if (args.length > 2 || args.length < 1) { //TODO: when running in intelliJ args[0] is not path to program
            String usage = "Usage: <language> [case_sensitive=false]";
            System.out.println(usage);
            System.exit(1);
        }
        args[1] = StringUtils.isEmpty(args[1]) ? "false" : "true";
        String[] stepsArgs = new String[args.length + 1];
        System.arraycopy(args, 0, stepsArgs, 0, args.length);
        System.out.println("stepsArgs" + Arrays.toString(stepsArgs));

        System.exit(0);

        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar(BUCKET_HOME_SCHEME + "step1.jar")
                .mainClass("step1UnigramCount")
                .args(args)
                .build();

        StepConfig step1Config = StepConfig.builder()
                .hadoopJarStep(step1)
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .name("step1")
                .build();

        HadoopJarStepConfig step2 = HadoopJarStepConfig.builder()
                .jar(BUCKET_HOME_SCHEME + "step2.jar")
                .mainClass("step2BigramDecadeCount")
                .args(args)
                .build();

        StepConfig step2Config = StepConfig.builder()
                .hadoopJarStep(step2)
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .name("step2")
                .build();

        HadoopJarStepConfig step3 = HadoopJarStepConfig.builder()
                .jar(BUCKET_HOME_SCHEME + "step3.jar")
                .mainClass("step3SortBigramsDecadeByOccurence")
                .args(args)
                .build();

        StepConfig step3Config = StepConfig.builder()
                .hadoopJarStep(step3)
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .name("step3")
                .build();

        String jobFlowId = ClusterOperations.createClusterWithSteps(emr,
                KEYS,
                BUCKET_HOME_SCHEME + "logs",
                INSTANCE_COUNT,
                step1Config, step2Config, step3Config);

        System.out.println("Ran JobFlow with id: " + jobFlowId);
    }
}
