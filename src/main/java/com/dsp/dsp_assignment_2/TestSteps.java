package com.dsp.dsp_assignment_2;

import com.dsp.mr_app.*;
import org.apache.log4j.Logger;


public class TestSteps {

    public static final Logger logger = Logger.getLogger(TestSteps.class);

    public static void main(String[] args) throws Exception {
        String[] pipe = new String[2];
        logger.info("im at teststeps");
        step1BigramDecadeCount.main(pipe);
        logger.info("about to start step 2");
        step2SortBigramsDecadeByOccurrence.main(pipe);
    }


}
