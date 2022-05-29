package com.dsp.models;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigramDecadeOccurrences extends Pair<BigramDecade, IntWritable> implements WritableComparable<Pair<BigramDecade, IntWritable>> {

    private Pair<BigramDecade, IntWritable> bigramDecadeOccurrencesPair;

    public BigramDecadeOccurrences(BigramDecade bigramDecade, IntWritable occurrences) {
        super(bigramDecade, occurrences);
        bigramDecadeOccurrencesPair = new Pair<>(bigramDecade, occurrences);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        bigramDecadeOccurrencesPair.getFirst().write(dataOutput);
        bigramDecadeOccurrencesPair.getSecond().write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        bigramDecadeOccurrencesPair.getFirst().readFields(dataInput);
        bigramDecadeOccurrencesPair.getSecond().readFields(dataInput);
    }

    @Override
    public int compareTo(Pair<BigramDecade, IntWritable> o) {
        /*
        Primary sort by IntWritable year
        Secondary sort by IntWritable occurrences
         */
        return  bigramDecadeOccurrencesPair.getFirst().getSecond().compareTo(o.getFirst().getSecond()) < 0 ? -1 :
                bigramDecadeOccurrencesPair.getFirst().getSecond().compareTo(o.getFirst().getSecond()) > 0 ? 1 :
                bigramDecadeOccurrencesPair.getSecond().compareTo(o.getSecond());
    }
}
