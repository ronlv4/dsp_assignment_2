package com.dsp.mr_app;

import com.dsp.models.Bigram;
import com.dsp.models.BigramDecade;
import com.dsp.models.BigramDecadeOccurrences;
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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class step1BigramDecadeCount {
    /*
    step1BigramDecadeCount Input:
        Key: line number of lzo file
        Value: w1 w2 TAB year TAB occurrences TAB booksRefs

    step1BigramDecadeCount Output:
        Key: <w1 w2:decade>
        Value: occurrences of the bigram <w1 w2> in the decade
     */

    public static final Logger logger = Logger.getLogger(step1BigramDecadeCount.class);
    public static final String BUCKET_HOME_SCHEME = "s3://dsp-assignment-2/";


    public static class BigramMapper extends Mapper<Object, Text, BigramDecade, IntWritable> {
        /*
        Mapper Input:
            Key: line number
            Value: w1 w2 TAB year TAB occurrences TAB booksRefs
        Mapper Output:
            Key: <w1 w2:decade>
            Value: occurrences of the bigram <w1 w2> in the year
         */

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("got from record reader the line " + value);
            String[] bigramLines = value.toString().split("\\R"); // bigram TAB year TAB occurrences TAB books
            Iterator<String> bigramItertor = Arrays.stream(bigramLines).iterator();
            String bigramLine;
            int year;
            IntWritable count;
            while (bigramItertor.hasNext()) {
                bigramLine = bigramItertor.next();
                logger.info("processing line " + bigramLine);
                String[] lineElements = bigramLine.split("\\t");
                if(lineElements[0].split("\\s").length < 2)
                    continue;
                Bigram bigram = new Bigram(new Text(lineElements[0].split("\\s")[0]), new Text(lineElements[0].split("\\s")[1]));
                try {
                    year = Integer.parseInt(lineElements[1]);
                    count = new IntWritable(Integer.parseInt((lineElements[2])));
                } catch (NumberFormatException ignored) {
                    continue;
                }
                IntWritable decade = new IntWritable(year / 10);
                logger.info("writing bigram '" + bigram + "', decade: " + decade + ", count: " + count);
                context.write(new BigramDecade(bigram, decade), count);
                // Set second to *
                Bigram bigram1 = new Bigram(new Text(lineElements[0].split("\\s")[0]), new Text("*"));
                context.write(new BigramDecade(bigram1, decade), count);
            }
        }
    }


    public static class IntSumCombiner extends Reducer<BigramDecade, IntWritable, BigramDecade, IntWritable> {
        /*
        Reducer Input:
            same as mapper output

        Reducer Output:
            Key: <w1 w2:decade>
            Value: occurrences of thr bigram <w1 w2> in the decade
         */
        private final IntWritable result = new IntWritable();


        public void reduce(BigramDecade key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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


    public static class IntSumReducer extends Reducer<BigramDecade, IntWritable, BigramDecade, Text> {
        /*
        Reducer Input:
            same as mapper output

        Reducer Output:
            Key: <w1 w2:decade>
            Value: occurrences of thr bigram <w1 w2> in the decade
         */
        private int total = 0;

        public void reduce(BigramDecade key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            logger.info("starting to count occurrences for " + key);
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(key.getBigram().getSecond().equals(new Text("*"))) {
                total = sum;
            }
            else {
                String concated = String.format("%d,%d", sum, total);
                context.write(key, new Text(concated));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 1) {
            logger.error("not place to store output path");
            System.exit(1);
        }
        logger.info("Starting " + step1BigramDecadeCount.class.getName() + " map reduce app");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step1BigramDecadeCount.class);
        job.setMapperClass(BigramMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(BigramDecade.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(BigramDecade.class);
        job.setOutputValueClass(Text.class);
        //FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data"));
//        FileInputFormat.addInputPath(job, new Path(BUCKET_HOME_SCHEME + "google-2grams/"));
        FileInputFormat.addInputPath(job, new Path("/home/hadoop/2grams-sample.txt"));
        //FileOutputFormat.setOutputPath(job, new Path("s3://dsp-assignment-2/output" + System.currentTimeMillis()));
        args[0] = "/home/hadoop/outputs/output" + System.currentTimeMillis();
//        args[0] = BUCKET_HOME_SCHEME + "output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.waitForCompletion(true);
        logger.info("Finished job 1 Successfully\n");
    }
}
