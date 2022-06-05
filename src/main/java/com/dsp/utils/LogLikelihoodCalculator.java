package com.dsp.utils;

public class LogLikelihoodCalculator {

    private final Integer c1;
    private final Integer c2;
    private final Integer c12;
    private final Integer N;

    public LogLikelihoodCalculator(Integer c1, Integer c2, Integer c12, Integer n) {
        this.c1 = c1;
        this.c2 = c2;
        this.c12 = c12;
        N = n;
    }

    public Double calculate() {
        double p = c2 / (double) N;
        double p1 = c12 / (double) c1;
        double p2 = (c2 - c12) / (double) (N - c1);
        return Math.log(L(c12, c1, p)) + Math.log(L(c2 - c12, N - c1, p)) - Math.log(L(c12, c1, p1)) - Math.log(L(c2 - c12, N - c1, p2));
    }

    private Double L(Integer k, Integer n, Double x) {
        return Math.pow(x, k) * Math.pow(1 - x, n - k);
    }
}
