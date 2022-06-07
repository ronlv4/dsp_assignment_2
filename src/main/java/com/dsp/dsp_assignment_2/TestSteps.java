package com.dsp.dsp_assignment_2;

import com.dsp.aws.emr.StepsOperations;
import com.dsp.mr_app.*;
import org.apache.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;

import java.util.UUID;

import static com.dsp.dsp_assignment_2.App.BUCKET_HOME_SCHEME;


public class TestSteps {

    public static final Logger logger = Logger.getLogger(TestSteps.class);
    public static final Region REGION = Region.US_EAST_1;

    public static void main(String[] args) throws Exception {

        EmrClient emr = EmrClient.builder().region(REGION).build();
        String[] pipe = new String[10];
        // Local
//        pipe[PathEnum.BASE_PATH.value] = "/home/hadoop/";
//        pipe[PathEnum.STOP_WORDS.value] = "/home/hadoop/stop-words/eng-stopwords.txt";
//        pipe[PathEnum.UNIGRAMS.value] = "/home/hadoop/1grams/1grams-sample.txt";
//        pipe[PathEnum.BIGRAMS.value] = "/home/hadoop/2grams/2grams-sample.txt";
//
//        pipe[PathEnum.STEP_1_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step1" + UUID.randomUUID();
//        pipe[PathEnum.STEP_2_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step2" + UUID.randomUUID();
//        pipe[PathEnum.STEP_3_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step3" + UUID.randomUUID();
//        pipe[PathEnum.STEP_4_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step4" + UUID.randomUUID();
//        pipe[PathEnum.STEP_5_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step5-final" + UUID.randomUUID();


        // cloud

        pipe[PathEnum.BASE_PATH.value - 1] = BUCKET_HOME_SCHEME;
        pipe[PathEnum.STEP_1_OUTPUT.value - 1] = BUCKET_HOME_SCHEME + "outputs/output-step1" + UUID.randomUUID();
        pipe[PathEnum.STEP_2_OUTPUT.value - 1] = BUCKET_HOME_SCHEME + "outputs/output-step2" + UUID.randomUUID();
        pipe[PathEnum.STEP_3_OUTPUT.value - 1] = BUCKET_HOME_SCHEME + "outputs/output-step3" + UUID.randomUUID();
        pipe[PathEnum.STEP_4_OUTPUT.value - 1] = BUCKET_HOME_SCHEME + "outputs/output-step4" + UUID.randomUUID();
        pipe[PathEnum.STEP_5_OUTPUT.value - 1] = BUCKET_HOME_SCHEME + "outputs/output-step5-final" + UUID.randomUUID();

        pipe[PathEnum.STOP_WORDS.value - 1] = BUCKET_HOME_SCHEME + "stop-words/eng-stopwords.txt";
//            pipe[PathEnum.UNIGRAMS.value-1] = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-1M/1gram/data";
//            pipe[PathEnum.BIGRAMS.value-1] = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-1M/2gram/data";
        pipe[PathEnum.UNIGRAMS.value - 1] = BUCKET_HOME_SCHEME + "1grams/1grams-sample.txt";
        pipe[PathEnum.BIGRAMS.value - 1] = BUCKET_HOME_SCHEME + "2grams/2grams-sample.txt";


        step1UnigramCount.main(pipe);
//        step2BigramDecadeCount.main(pipe);
//        step3MergeUnigramsBigramsLeft.main(pipe);
//        step4MergeUnigramsBigramsRight.main(pipe);
//        step5CalculateLogLikelihood.main(pipe);
    }


}
