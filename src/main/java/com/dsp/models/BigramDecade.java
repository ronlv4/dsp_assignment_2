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
    private Text bigram;
    private IntWritable decade;

    public static final Logger logger = Logger.getLogger(BigramDecade.class);

    public BigramDecade() {
        set(new Text(), new IntWritable());
    }

    public BigramDecade(Text bigram, IntWritable decade) {
        set(bigram, decade);
    }

    public void set(Text bigram, IntWritable decade) {
        this.bigram = bigram;
        this.decade = decade;
    }

    public static BigramDecade fromString(String bigram){ // w1 w2:decade
        try {
            return new BigramDecade(new Text(bigram.split(":")[0]), new IntWritable(Integer.parseInt(bigram.split(":")[1])));
        }catch (Exception e){
            logger.error("unable to create a bigram decade out of " + bigram);
            throw e;
        }
    }

    public Text getBigram() {
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
        return  bigram.compareTo(o.getBigram()) < 0 ? -1 :
                bigram.compareTo(o.getBigram()) > 0 ? 1 :
                decade.compareTo(o.getDecade());
    }
    @Override
    public String toString(){
        return bigram + ":" + decade;
    }
}
