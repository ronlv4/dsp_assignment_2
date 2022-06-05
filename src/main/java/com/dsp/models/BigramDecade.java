package com.dsp.models;

import com.dsp.mr_app.step2SortBigramsDecadeByOccurrence;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigramDecade implements WritableComparable<BigramDecade>{
    private Bigram bigram;
    private IntWritable decade;

    public static final Logger logger = Logger.getLogger(BigramDecade.class);

    public BigramDecade() {
        set(new Bigram(), new IntWritable());
    }

    public BigramDecade(Bigram bigram, IntWritable decade) {
        set(bigram, decade);
    }

    public void set(Bigram bigram, IntWritable decade) {
        this.bigram = bigram;
        this.decade = decade;
    }

    public static BigramDecade fromString(String bigram){ // w1 w2:decade
        try {
            Bigram b = new Bigram(new Text(bigram.split(":")[0].split(" ")[0]),
                    new Text(bigram.split(":")[0].split(" ")[1]));
            return new BigramDecade(b, new IntWritable(Integer.parseInt(bigram.split(":")[1])));
        }catch (Exception e){
            logger.error("unable to create a bigram decade out of " + bigram);
            throw e;
        }
    }

    public Bigram getBigram() {
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

    public int compareTo(BigramDecade o) {
        /*
        Primary sort by Text bigram
        Secondary sort by IntWritable year
         */
//        return  bigram.compareTo(o.getBigram()) < 0 ? -1 :
//                bigram.compareTo(o.getBigram()) > 0 ? 1 :
//                decade.compareTo(o.getDecade());

        return  decade.compareTo(o.getDecade()) != 0 ? decade.compareTo(o.getDecade()) :
                bigram.compareTo(o.getBigram());
    }
    @Override
    public String toString(){
        return bigram + ":" + decade;
    }
}
