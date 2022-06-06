package com.dsp.models;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReverseBigramDecade implements WritableComparable<ReverseBigramDecade> {
    private ReverseBigram bigram;
    private IntWritable decade;

    public static final Logger logger = Logger.getLogger(ReverseBigramDecade.class);

    public ReverseBigramDecade() {
        set(new ReverseBigram(), new IntWritable());
    }

    public ReverseBigramDecade(ReverseBigram bigram, IntWritable decade) {
        set(bigram, decade);
    }

    public void set(ReverseBigram bigram, IntWritable decade) {
        this.bigram = bigram;
        this.decade = decade;
    }

    public static ReverseBigramDecade fromString(String bigram){ // w1 w2:decade
        try {
            ReverseBigram b = new ReverseBigram(new Text(bigram.split(":")[0].split(" ")[0]),
                    new Text(bigram.split(":")[0].split(" ")[1]));
            return new ReverseBigramDecade(b, new IntWritable(Integer.parseInt(bigram.split(":")[1])));
        }catch (Exception e){
            logger.error("unable to create a bigram decade out of " + bigram);
            throw e;
        }
    }

    public ReverseBigram getBigram() {
        return bigram;
    }

    public IntWritable getDecade() {
        return decade;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        bigram.write(dataOutput);
        decade.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        bigram.readFields(dataInput);
        decade.readFields(dataInput);
    }

    public int compareTo(ReverseBigramDecade o) {
        /*
        Primary sort by year
        Secondary sort by bigram
         */

        return  decade.compareTo(o.getDecade()) != 0 ? decade.compareTo(o.getDecade()) :
                bigram.compareTo(o.getBigram());
    }
    @Override
    public String toString(){
        return bigram + ":" + decade;
    }
}
