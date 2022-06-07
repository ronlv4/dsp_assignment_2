package com.dsp.dsp_assignment_2;

import com.dsp.mr_app.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.Arrays;


public class TestSteps {

    public static final Logger logger = Logger.getLogger(TestSteps.class);

    public static void main(String[] args) throws Exception {

        logger.debug("args in: " + Arrays.toString(args));

        if (args.length > 3 || args.length < 2) {
            String usage = "Usage: <language> [case_sensitive=false]";
            System.out.println(usage);
            System.exit(1);
        }
        args[2] = StringUtils.isEmpty(args[1]) ? "false" : "true";
        logger.debug("args out: " + Arrays.toString(args));

        try {
            step1UnigramCount.main(args);
            step2BigramDecadeCount.main(args);
            step3MergeUnigramsBigramsLeft.main(args);
            step4MergeUnigramsBigramsRight.main(args);
            step5CalculateLogLikelihood.main(args);
        } catch (Exception e){
            logger.error(e.getMessage(), e);
            throw e;
        }
    }


}
