package com.dsp.models;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigramDecadeOccurrences implements WritableComparable<BigramDecadeOccurrences> {

    private BigramDecade bigramDecade;
    private IntWritable occurrences;

    public BigramDecadeOccurrences() {
        bigramDecade = new BigramDecade();
        occurrences = new IntWritable();
    }

    public BigramDecadeOccurrences(BigramDecade bigramDecade, IntWritable occurrences) {
        this.bigramDecade = bigramDecade;
        this.occurrences = occurrences;
    }

    public BigramDecade getBigramDecade() {
        return bigramDecade;
    }

    public IntWritable getOccurrences() {
        return occurrences;
    }

    @Override
    public int compareTo(BigramDecadeOccurrences o) {
        /*
        Primary sort by IntWritable decade
        Secondary sort by IntWritable occurrences
         */
        return  bigramDecade.getDecade().compareTo(o.getBigramDecade().getDecade()) < 0 ? -1 :
                bigramDecade.getDecade().compareTo(o.getBigramDecade().getDecade()) > 0 ? 1 :
                occurrences.compareTo(o.getOccurrences());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        bigramDecade.write(dataOutput);
        occurrences.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        bigramDecade.readFields(dataInput);
        occurrences.readFields(dataInput);
    }

    @Override
    public String toString() {
        return bigramDecade + ":" + occurrences;
    }
}
