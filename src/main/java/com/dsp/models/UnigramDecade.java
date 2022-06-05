package com.dsp.models;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UnigramDecade implements WritableComparable<UnigramDecade> {
    private Unigram unigram;
    private IntWritable decade;

    public UnigramDecade() {
        set(new Unigram(), new IntWritable());
    }

    public UnigramDecade(Unigram unigram, IntWritable decade) {
        set(unigram, decade);
    }

    public void set(Unigram unigram, IntWritable decade) {
        this.unigram = unigram;
        this.decade = decade;
    }

    public static UnigramDecade fromString(String unigram) { // w1 w2:decade
        return new UnigramDecade(Unigram.fromString(unigram.split("\\s")[0]), new IntWritable(Integer.parseInt(unigram.split("\\s")[1])));
    }

    public Unigram getUnigram() {
        return unigram;
    }

    public IntWritable getDecade() {
        return decade;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        unigram.write(dataOutput);
        decade.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        unigram.readFields(dataInput);
        decade.readFields(dataInput);
    }

    public int compareTo(UnigramDecade o) {
        /*
        Primary sort by Unigram unigram
        Secondary sort by IntWritable year
         */
        return  unigram.compareTo(o.getUnigram()) < 0 ? -1 :
                unigram.compareTo(o.getUnigram()) > 0 ? 1 :
                decade.compareTo(o.getDecade());
    }

    @Override
    public String toString() {
        return unigram + ":" + decade;
    }
}
