package com.dsp.dsp_assignment_2;

import com.dsp.mr_app.*;

import java.io.IOException;

public class TestSteps {

    public static void main(String[] args) throws Exception {
        String[] pipe = new String[2];
        step1BigramDecadeCount.main(pipe);
        step2SortBigramsDecadeByOccurrence.main(pipe);
    }


}
