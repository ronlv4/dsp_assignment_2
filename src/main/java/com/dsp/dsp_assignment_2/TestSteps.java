package com.dsp.dsp_assignment_2;

import com.dsp.mr_app.*;
import org.apache.log4j.Logger;


public class TestSteps {

    public static final Logger logger = Logger.getLogger(TestSteps.class);

    public static void main(String[] args) throws Exception {
        String[] pipe = new String[8];
        pipe[0] = "/home/hadoop/";
        pipe[1] = "/home/hadoop/stop-words/eng-stopwords.txt";
        pipe[2] = "/home/hadoop/google-1grams/1grams-sample.txt";
        pipe[3] = "/home/hadoop/2grams-sample.txt";
        step1UnigramCount.main(pipe);
        step2BigramDecadeCount.main(pipe);
        step3MergeUnigramsBigramsLeft.main(pipe);
        step4MergeUnigramsBigramsRight.main(pipe);
        step5CalculateLogLikelihood.main(pipe);
    }


}
