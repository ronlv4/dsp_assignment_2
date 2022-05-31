package com.dsp.mr_app;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import com.dsp.models.BigramDecade;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;

public class step1BigramDecadeCount {

    public static final Logger logger = Logger.getLogger(step1BigramDecadeCount.class);

    public static class BigramMapper extends Mapper<Object, Text, BigramDecade, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println("got from record reader the line " + value.toString());
            logger.info("got from record reader the line " + value);
            String[] bigramsLine = value.toString().split("\\R"); // bigram TAB year TAB occurrences TAB books
            //System.out.println("splitted line %s into:\n" + Arrays.toString(bigramsLine));
            logger.info("splitted line into: " + Arrays.toString(bigramsLine));
            Iterator<String> bigramItertor = Arrays.stream(bigramsLine).iterator();
            String bigramLine;
            int year;
            IntWritable count;
            while (bigramItertor.hasNext()) {
                bigramLine = bigramItertor.next();
                //System.out.println("processing line " + bigramLine);
                logger.info("processing line " + bigramLine);
                String[] lineElements = bigramLine.split("\\t");
                //System.out.println("splitted line into:\n" + Arrays.toString(lineElements));
                logger.info("splitted line %s into: " + Arrays.toString(lineElements));
                Text bigram = new Text(lineElements[0]);
                //System.out.println("the bigram is " + bigram);
                logger.info("the bigram is " + bigram);
                try {
                    year = Integer.parseInt(lineElements[1]);
                    count = new IntWritable(Integer.parseInt((lineElements[2])));
                    //System.out.println("the year is " + year);
                    //System.out.println("the count is " + count.get());
                    logger.info("the year is " + year);
                    logger.info("the count is " + count.get());
                } catch (NumberFormatException ignored) {
                    continue;
                }
                IntWritable decade = new IntWritable(year / 10);
                //System.out.println("the decade is " + decade.get());
                logger.info("the decade is " + decade.get());
                context.write(new BigramDecade(bigram, decade), count);
            }
        }
    }

    public static class IntSumReducer extends Reducer<BigramDecade, IntWritable, BigramDecade, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(BigramDecade key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            logger.info("starting to count occurrences for " + key);
            for (IntWritable val : values) {
                sum += val.get();
            }
            logger.info("counted: " + sum);
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 1) {
            System.out.println("not place to store output path");
            System.exit(1);
        }
        System.out.println("Starting " + step1BigramDecadeCount.class.getName() + " map reduce app");
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step1BigramDecadeCount.class);
        job.setMapperClass(BigramMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(BigramDecade.class);
        job.setOutputValueClass(IntWritable.class);
        //FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data"));
        FileInputFormat.addInputPath(job, new Path("/home/hadoop/input/"));
        //FileOutputFormat.setOutputPath(job, new Path("s3://dsp-assignment-2/output" + System.currentTimeMillis()));
        args[0] = "/home/hadoop/output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.waitForCompletion(true);
        System.out.println("Finished job 1 Successfully\n");
    }
}
