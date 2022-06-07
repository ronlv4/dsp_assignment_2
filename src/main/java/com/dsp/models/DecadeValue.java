package com.dsp.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.javatuples.Decade;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DecadeValue implements WritableComparable<DecadeValue>{
    private IntWritable decade;
    private DoubleWritable value;


    public static final Logger logger = Logger.getLogger(DecadeValue.class);

    public DecadeValue() {
        set(new IntWritable(), new DoubleWritable());
    }

    public DecadeValue(IntWritable decade, DoubleWritable value) {
        set(decade, value);
    }

    public void set(IntWritable decade, DoubleWritable value) {
        this.decade = decade;
        this.value = value;
    }

    public static DecadeValue fromString(String decadeValue){ // w1 w2:decade
        try {
            IntWritable decade = new IntWritable(Integer.parseInt(decadeValue.split(":")[0]));
            DoubleWritable value = new DoubleWritable(Double.parseDouble(decadeValue.split(":")[1]));
            return new DecadeValue(decade, value);
        }catch (Exception e){
            logger.error("unable to create a decade value out of " + decadeValue);
            throw e;
        }
    }

    public DoubleWritable getValue() {
        return value;
    }

    public IntWritable getDecade() {
        return decade;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        decade.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade.readFields(dataInput);
        value.readFields(dataInput);
    }

    public int compareTo(DecadeValue o) {
        /*
        Primary sort by decade
        Secondary sort by value
         */

        return  decade.compareTo(o.getDecade()) != 0 ? decade.compareTo(o.getDecade()) :
                -1 * value.compareTo(o.getValue());
    }
    @Override
    public String toString(){
        return decade + ":" + value;
    }
}
