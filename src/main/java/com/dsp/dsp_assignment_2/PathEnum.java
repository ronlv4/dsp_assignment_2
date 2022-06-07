package com.dsp.dsp_assignment_2;

public enum PathEnum {
    BASE_PATH(0),
    STOP_WORDS(1),
    UNIGRAMS(2),
    BIGRAMS(3),
    STEP_1_OUTPUT(4),
    STEP_2_OUTPUT(5),
    STEP_3_OUTPUT(6),
    STEP_4_OUTPUT(7);


    public final int value;

    private PathEnum(int value) {
        this.value = value;
    }
}
