package com.dsp.dsp_assignment_2;

import com.dsp.mr_app.*;


public class TestSteps {

    public static void main(String[] args) throws Exception {
        String[] pipe = new String[2];
        System.out.println("im at teststeps");
        step1BigramDecadeCount.main(pipe);
        System.out.println("about to start step 2");
        step2SortBigramsDecadeByOccurrence.main(pipe);
    }


}
