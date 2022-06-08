package com.dsp.dsp_assignment_2;

public enum PathEnum {
    BASE_PATH(1),
    STOP_WORDS(2),
    UNIGRAMS(3),
    BIGRAMS(4),
    STEP_1_OUTPUT(5),
    STEP_2_OUTPUT(6),
    STEP_3_OUTPUT(7),
    STEP_4_OUTPUT(8),
    STEP_5_OUTPUT(9),

    LANG(10);


    public final int value;

    private PathEnum(int value) {
        this.value = value;
    }
}
