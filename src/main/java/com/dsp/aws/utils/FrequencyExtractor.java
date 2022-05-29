package com.dsp.aws.utils;

import software.amazon.awssdk.utils.IoUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

public class FrequencyExtractor {

    private final static HashMap<Integer, Long> frequencies = new HashMap<>();

    public FrequencyExtractor() {
    }

    private static void splitter(String yearFreq) {
        int year;
        long freq;
        try {
            year = Integer.parseInt(yearFreq.split(",")[0]);
            freq = Long.parseLong(yearFreq.split(",")[1]);
        } catch (NumberFormatException ignored) {
            return;
        }
        frequencies.put(year, freq);
    }

    public static void setUp() {
        String freqData = "";
        try {
            freqData = IoUtils.toUtf8String(Objects.requireNonNull(FrequencyExtractor.class.getResourceAsStream("/eng-all-totalcounts.txt")));
        } catch (IOException ignored) {
        }
        Arrays.stream(freqData.split("\\t")).forEach(FrequencyExtractor::splitter);
    }
}
