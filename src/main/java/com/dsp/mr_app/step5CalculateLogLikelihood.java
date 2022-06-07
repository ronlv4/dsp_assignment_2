package com.dsp.mr_app;

import com.dsp.dsp_assignment_2.PathEnum;
import com.dsp.models.DecadeValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class step5CalculateLogLikelihood {

    public static final Logger logger = Logger.getLogger(step5CalculateLogLikelihood.class);
    public static final String BUCKET_HOME_SCHEME = "s3://dsp-assignment-2/";


    public static class LogMapper extends Mapper<Object, Text, DecadeValue, Text> {
        private double calcL(double k, double n, double x) {
            return Math.pow(x, k) * Math.pow(1 - x, n - k);
        }

        private double calcLogLikelihood(double c1, double c2, double c12, double N) {

            double p = c2 / N;
            double p1 = c12 / c1;
            double p2 = (c2 - c12) / (N - c1);

            return Math.log(calcL(c12, c1, p)) + Math.log(calcL(c2 - c12, N - c1, p)) -
                    Math.log(calcL(c12, c1, p1)) - Math.log(calcL(c2 - c12, N - c1, p2));
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("got from record reader the line " + value);
            String[] keyValue = value.toString().split("\\t");
            String oldKey = keyValue[0];
            String val = keyValue[1];
            String[] nums = val.split(",");
            double c12 = Integer.parseInt(nums[0]);
            double N = Integer.parseInt(nums[1]);
            double c1 = Integer.parseInt(nums[2]);
            double c2 = Integer.parseInt(nums[3]);

            double logLikelihood = calcLogLikelihood(c1, c2, c12, N);

            int decade = Integer.parseInt(oldKey.split(":")[1]);

            context.write(new DecadeValue(new IntWritable(decade), new DoubleWritable(logLikelihood)), new Text(oldKey));
        }
    }


    public static class LogReducer extends Reducer<DecadeValue, Text, Text, DoubleWritable> {

        int decadeCount = 0;
        int currentDecade = 0;
        private final HashMap<Integer, Integer> countMap = new HashMap<>();

        public void reduce(DecadeValue key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            logger.info("starting to count occurrences for " + key);
            Text val = values.iterator().next();
            logger.info("value is " + val.toString());
            int decade = key.getDecade().get();
            if(currentDecade != decade) {
                currentDecade = decade;
                decadeCount = 0;
            }

            if(decadeCount >= 100)
                return;

            decadeCount++;
            context.write(val, key.getValue());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 1) {
            logger.error("not place to store output path");
            System.exit(1);
        }
        logger.info("Starting " + step5CalculateLogLikelihood.class.getName() + " map reduce app");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step5CalculateLogLikelihood.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        job.setMapOutputKeyClass(DecadeValue.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[PathEnum.STEP_4_OUTPUT.value]));
        FileOutputFormat.setOutputPath(job, new Path(args[PathEnum.BASE_PATH.value] + "outputs/final_output" + System.currentTimeMillis()));
        int done = job.waitForCompletion(true) ? 0 : 1;
        if (done == 1)
            System.exit(1);
    }
}
