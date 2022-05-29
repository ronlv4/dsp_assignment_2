package com.dsp.models;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

public class CollocationCount implements WritableComparable<Pair<Text, LongWritable>> {

    private Pair<Text, LongWritable> collocationCountPair;

    public CollocationCount(Text w1, Text w2, LongWritable occurrence) {
        Text collocation = new Text(w1.toString() + " " + w2.toString());
        collocationCountPair = new Pair<>(collocation, occurrence);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        collocationCountPair.getFirst().write(dataOutput);
        collocationCountPair.getSecond().write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        collocationCountPair.getFirst().readFields(dataInput);
        collocationCountPair.getSecond().readFields(dataInput);
    }

    @Override
    public int compareTo(Pair<Text, LongWritable> other) {
        return collocationCountPair.getSecond().compareTo(other.getSecond());
    }
}
