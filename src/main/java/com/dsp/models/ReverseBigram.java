package com.dsp.models;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


public class ReverseBigram implements WritableComparable<ReverseBigram> {
    private Text first;
    private Text second;

    public static final Logger logger = Logger.getLogger(ReverseBigram.class);

    public ReverseBigram() {
        set(new Text(), new Text());
    }

    public ReverseBigram(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public static ReverseBigram fromString(String bigram){ // w1 w2:decade
        try {
            return new ReverseBigram(new Text(bigram.split(" ")[0]), new Text(bigram.split(" ")[1]));
        }catch (Exception e){
            logger.error("unable to create a bigram  out of " + bigram);
            throw e;
        }
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public int compareTo(ReverseBigram o) {
        /*
        Primary sort by second
        Secondary sort by first (* is smallest)
         */
        return second.compareTo(o.getSecond()) != 0 ? second.compareTo(o.getSecond()) :
                (first.equals(new Text("*")) && !o.getFirst().equals(new Text("*"))) ? -1 :
                        (!first.equals(new Text("*")) && o.getFirst().equals(new Text("*"))) ? 1 :
                                first.compareTo(o.getFirst());
    }
    @Override
    public String toString(){
        return first + " " + second;
    }
}
