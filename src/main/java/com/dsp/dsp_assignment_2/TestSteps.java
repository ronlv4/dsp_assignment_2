package com.dsp.dsp_assignment_2;

import com.dsp.mr_app.*;
import org.apache.log4j.Logger;

import java.util.UUID;


public class TestSteps {

    public static final Logger logger = Logger.getLogger(TestSteps.class);

    public static void main(String[] args) throws Exception {
        String[] pipe = new String[10];
        pipe[PathEnum.BASE_PATH.value] = "/home/hadoop/";
        pipe[PathEnum.STOP_WORDS.value] = "/home/hadoop/stop-words/eng-stopwords.txt";
        pipe[PathEnum.UNIGRAMS.value] = "/home/hadoop/1grams/1grams-sample.txt";
        pipe[PathEnum.BIGRAMS.value] = "/home/hadoop/2grams/2grams-sample.txt";

        pipe[PathEnum.STEP_1_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step1" + UUID.randomUUID();
        pipe[PathEnum.STEP_2_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step2" + UUID.randomUUID();
        pipe[PathEnum.STEP_3_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step3" + UUID.randomUUID();
        pipe[PathEnum.STEP_4_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step4" + UUID.randomUUID();
        pipe[PathEnum.STEP_5_OUTPUT.value] = "/home/hadoop/" + "outputs/output-step5-final" + UUID.randomUUID();

        step1UnigramCount.main(pipe);
//        step2BigramDecadeCount.main(pipe);
//        step3MergeUnigramsBigramsLeft.main(pipe);
//        step4MergeUnigramsBigramsRight.main(pipe);
//        step5CalculateLogLikelihood.main(pipe);
    }


}
