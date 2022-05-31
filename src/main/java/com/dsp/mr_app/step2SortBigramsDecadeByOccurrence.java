package com.dsp.mr_app;

import com.dsp.models.BigramDecade;
import com.dsp.models.BigramDecadeOccurrences;
import com.dsp.models.CollocationCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

public class step2SortBigramsDecadeByOccurrence {

    public static final Logger logger = Logger.getLogger(step2SortBigramsDecadeByOccurrence.class);

    public static class TokenizerMapper extends Mapper<Object, BigramDecade, BigramDecadeOccurrences, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text w1 = new Text();
        private Text w2 = new Text();

        public void map(Object key, BigramDecadeOccurrences bigramDecadeOccurrences, Context context) throws IOException, InterruptedException {
            logger.info("got from record reader the line " + bigramDecadeOccurrences);


//            while (itr.hasMoreTokens()) {
//                w1.set(itr.nextToken());
//                CollocationCount pair = new CollocationCount(w1, )
//                context.write(pair, one);
//            }
        }
    }

    public static class IntSumReducer extends Reducer<CollocationCount, IntWritable, Text, BigramDecadeOccurrences> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<BigramDecadeOccurrences> values, Context context) throws IOException, InterruptedException {
            int currentDecade;
            int takes = 0;
            Iterator<BigramDecadeOccurrences> iter = values.iterator();
            while (iter.hasNext() && takes++ <= 100) {
                context.write(key, iter.next());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting " + step1BigramDecadeCount.class.getName() + " map reduce app");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step1BigramDecadeCount.class);
        job.setMapperClass(step1BigramDecadeCount.BigramMapper.class);
        job.setCombinerClass(step1BigramDecadeCount.IntSumReducer.class);
        job.setReducerClass(step1BigramDecadeCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-assignment-2/output" + System.currentTimeMillis()));
        FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/output" + System.currentTimeMillis()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("Finished job Successfully\n");
    }
}
