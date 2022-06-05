package com.dsp.models;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Unigram implements WritableComparable<Unigram> {
    private Text unigram;

    public static final Logger logger = Logger.getLogger(BigramDecade.class);

    public Unigram() {
        unigram = new Text();
    }

    public Unigram(Text unigram) {
        this.unigram = unigram;
    }

    public static Unigram fromString(String unigram) {
        try {
            return new Unigram(new Text(unigram));
        } catch (Exception e) {
            logger.error("unable to create a unigram  out of " + unigram);
            throw e;
        }
    }

    public Text getUnigram() {
        return unigram;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        unigram.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        unigram.readFields(dataInput);
    }

    @Override
    public int compareTo(Unigram o) {
        /*
        sort by Text - enforce * is the smallest
         */
        return  unigram.toString().equals("*") && !o.getUnigram().toString().equals("*") ? -1 :
                !unigram.toString().equals("*") && o.getUnigram().toString().equals("*") ? 1 :
                unigram.compareTo(o.getUnigram());
    }

    @Override
    public String toString() {
        return unigram.toString();
    }
}
