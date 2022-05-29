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

public class step1BigramDecadeCount {
    public static class BigramMapper
            extends Mapper<Object, Text, BigramDecade, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] bigramsLine = value.toString().split("\\R"); // bigram tringTAB year TAB occurrences TAB books
            Iterator<String> bigramItertor = Arrays.stream(bigramsLine).iterator();
            String bigramLine;
            while (bigramItertor.hasNext()) {
                bigramLine = bigramItertor.next();
                String[] lineElements = bigramLine.split("\\t");
                Text bigram = new Text(lineElements[0]);
                int year = Integer.parseInt(lineElements[1]);
                IntWritable count = new IntWritable(Integer.parseInt((lineElements[2])));
                IntWritable decade = new IntWritable(year / 10);
                context.write(new BigramDecade(bigram, decade), count);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<BigramDecade, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting " + step1BigramDecadeCount.class.getName() + " map reduce app");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step1BigramDecadeCount.class);
        job.setMapperClass(BigramMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data"));
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-assignment-2/output" + System.currentTimeMillis()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("Finished job Successfully\n");
    }
}
