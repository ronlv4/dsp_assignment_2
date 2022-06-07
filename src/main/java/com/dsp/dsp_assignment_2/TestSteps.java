package com.dsp.dsp_assignment_2;

import com.dsp.mr_app.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.Arrays;


public class TestSteps {

    public static final Logger logger = Logger.getLogger(TestSteps.class);

    public static void main(String[] args) throws Exception {

        logger.debug("args" + Arrays.toString(args));

        if (args.length > 2 || args.length < 1) { //TODO: when running in intelliJ args[0] is not path to program
            String usage = "Usage: <language> [case_sensitive=false]";
            System.out.println(usage);
            System.exit(1);
        }
        args[1] = StringUtils.isEmpty(args[1]) ? "false" : "true";
        String[] stepsArgs = new String[args.length + 1];
        System.arraycopy(args, 0, stepsArgs, 0, args.length);
        logger.debug("stepsArgs" + Arrays.toString(stepsArgs));

        try {
            String[] pipe = new String[3];
            step1UnigramCount.main(pipe);
            step2BigramDecadeCount.main(pipe);
            step3MergeUnigramsBigramsLeft.main(pipe);
            step4MergeUnigramsBigramsRight.main(pipe);
            step5CalculateLogLikelihood.main(pipe);
        } catch (Exception e){
            logger.error(e.getMessage(), e);
            throw e;
        }
    }


}
