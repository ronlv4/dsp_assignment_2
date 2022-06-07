package com.dsp.dsp_assignment_2;

import com.dsp.aws.emr.ClusterOperations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.UUID;

public class App {
    public static final String BUCKET_HOME_SCHEME = "s3://dsp-assignment-2-shir/";
    public static final Region REGION = Region.US_EAST_1;
    public static final String KEYS = "dist1";
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


        // System.exit(0); // END OF TESTING SECTION
        // Real App:

        if (args.length != 1) {
            String usage = "Usage: <language>";
            System.out.println(usage);
            System.exit(1);
        }

        String[] pipe = new String[9];
        String lang = args[0];
        pipe[PathEnum.BASE_PATH.value-1] = BUCKET_HOME_SCHEME;
        pipe[PathEnum.STEP_1_OUTPUT.value-1] = BUCKET_HOME_SCHEME + "outputs/output" + UUID.randomUUID();
        pipe[PathEnum.STEP_2_OUTPUT.value-1] = BUCKET_HOME_SCHEME + "outputs/output" + UUID.randomUUID();
        pipe[PathEnum.STEP_3_OUTPUT.value-1] = BUCKET_HOME_SCHEME + "outputs/output" + UUID.randomUUID();
        pipe[PathEnum.STEP_4_OUTPUT.value-1] = BUCKET_HOME_SCHEME + "outputs/output" + UUID.randomUUID();
        pipe[PathEnum.STEP_5_OUTPUT.value-1] = BUCKET_HOME_SCHEME + "outputs/output" + UUID.randomUUID();

        if (lang.equals("heb")){
            pipe[PathEnum.STOP_WORDS.value-1] = BUCKET_HOME_SCHEME + "heb-stopwords.txt";
//            pipe[PathEnum.UNIGRAMS.value-1] = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
//            pipe[PathEnum.BIGRAMS.value-1] = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
            pipe[PathEnum.UNIGRAMS.value-1] = BUCKET_HOME_SCHEME + "1gram-sample.txt";
            pipe[PathEnum.BIGRAMS.value-1] = BUCKET_HOME_SCHEME + "2grams-sample.txt";
        }
        else {
            pipe[PathEnum.STOP_WORDS.value-1] = BUCKET_HOME_SCHEME + "eng-stopwords.txt";
//            pipe[PathEnum.UNIGRAMS.value-1] = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data";
//            pipe[PathEnum.BIGRAMS.value-1] = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
            pipe[PathEnum.UNIGRAMS.value-1] = BUCKET_HOME_SCHEME + "1gram-sample.txt";
            pipe[PathEnum.BIGRAMS.value-1] = BUCKET_HOME_SCHEME + "2grams-sample.txt";
        }


        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar(BUCKET_HOME_SCHEME + "step1.jar")
                .mainClass("step1UnigramCount")
                .args(pipe)
                .build();

        StepConfig step1Config = StepConfig.builder()
                .hadoopJarStep(step1)
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .name("step1")
                .build();

        HadoopJarStepConfig step2 = HadoopJarStepConfig.builder()
                .jar(BUCKET_HOME_SCHEME + "step2.jar")
                .mainClass("step2BigramDecadeCount")
                .args(pipe)
                .build();

        StepConfig step2Config = StepConfig.builder()
                .hadoopJarStep(step2)
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .name("step2")
                .build();

        HadoopJarStepConfig step3 = HadoopJarStepConfig.builder()
                .jar(BUCKET_HOME_SCHEME + "step3.jar")
                .mainClass("step3SortBigramsDecadeByOccurence")
                .args(pipe)
                .build();

        StepConfig step3Config = StepConfig.builder()
                .hadoopJarStep(step3)
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .name("step3")
                .build();

        HadoopJarStepConfig step4 = HadoopJarStepConfig.builder()
                .jar(BUCKET_HOME_SCHEME + "step4.jar")
                .mainClass("step4MergeUnigramsBigramsRight")
                .args(pipe)
                .build();

        StepConfig step4Config = StepConfig.builder()
                .hadoopJarStep(step4)
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .name("step4")
                .build();

        HadoopJarStepConfig step5 = HadoopJarStepConfig.builder()
                .jar(BUCKET_HOME_SCHEME + "step5.jar")
                .mainClass("step5CalculateLogLikelihood")
                .args(pipe)
                .build();

        StepConfig step5Config = StepConfig.builder()
                .hadoopJarStep(step5)
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .name("step5")
                .build();


        String jobFlowId = ClusterOperations.createClusterWithSteps(emr,
                KEYS,
                BUCKET_HOME_SCHEME + "logs",
                INSTANCE_COUNT,
                step1Config, step2Config, step3Config, step4Config, step5Config);

        System.out.println("Ran JobFlow with id: " + jobFlowId);
    }
}
