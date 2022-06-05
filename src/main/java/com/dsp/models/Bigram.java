package com.dsp.models;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


public class Bigram implements WritableComparable<Bigram> {
    private Text first;
    private Text second;

    public static final Logger logger = Logger.getLogger(BigramDecade.class);

    public Bigram() {
        set(new Text(), new Text());
    }

    public Bigram(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public static Bigram fromString(String bigram){ // w1 w2:decade
        try {
            return new Bigram(new Text(bigram.split(" ")[0]), new Text(bigram.split(" ")[1]));
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

    public int compareTo(Bigram o) {
        /*
        Primary sort by first
        Secondary sort by second (* is smallest)
         */
        return first.compareTo(o.getFirst()) != 0 ? first.compareTo(o.getFirst()) :
                (second.equals(new Text("*")) && !o.getSecond().equals(new Text("*"))) ? -1 :
                        (!second.equals(new Text("*")) && o.getSecond().equals(new Text("*"))) ? 1 :
                                second.compareTo(o.getSecond());
    }
    @Override
    public String toString(){
        return first + " " + second;
    }
}
