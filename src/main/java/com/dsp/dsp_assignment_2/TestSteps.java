package com.dsp.dsp_assignment_2;

import com.dsp.mr_app.*;
import org.apache.log4j.Logger;


public class TestSteps {

    public static final Logger logger = Logger.getLogger(TestSteps.class);

    public static void main(String[] args) throws Exception {
        String[] pipe = new String[3];
        step1UnigramCount.main(pipe);
        step2BigramDecadeCount.main(pipe);
        step3MergeUnigramsBigramsLeft.main(pipe);
        step4MergeUnigramsBigramsRight.main(pipe);
        step5CalculateLogLikelihood.main(pipe);
    }


}
