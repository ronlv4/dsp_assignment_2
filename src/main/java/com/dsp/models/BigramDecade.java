package com.dsp.models;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigramDecade extends Pair<Text, IntWritable> implements WritableComparable<Pair<Text, IntWritable>>{

    private Pair<Text, IntWritable> bigramDecadePair;

    public BigramDecade() {
        super(new Text(), new IntWritable());
    }

    public BigramDecade(Text bigram, IntWritable year) {
        super(bigram, year);
        bigramDecadePair = new Pair<>(bigram, year);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        bigramDecadePair.getFirst().write(dataOutput);
        bigramDecadePair.getSecond().write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        bigramDecadePair.getFirst().readFields(dataInput);
        bigramDecadePair.getSecond().readFields(dataInput);
    }

    @Override
    public int compareTo(Pair<Text, IntWritable> o) {
        /*
        Primary sort by Text bigram
        Secondary sort by IntWritable year
         */
        return  bigramDecadePair.getFirst().compareTo(o.getFirst()) < 0 ? -1 :
                bigramDecadePair.getFirst().compareTo(o.getFirst()) > 0 ? 1 :
                bigramDecadePair.getSecond().compareTo(o.getSecond());
    }
}
