package com.dsp.mr_app;

import com.dsp.models.BigramDecade;
import com.dsp.models.BigramDecadeOccurrences;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class step2SortBigramsDecadeByOccurrence {

    public static final Logger logger = Logger.getLogger(step2SortBigramsDecadeByOccurrence.class);
    private static IntWritable one = new IntWritable(1);

    public static class BigramOccurrencesMapper extends Mapper<Object, Text, BigramDecadeOccurrences, IntWritable> {

        private Text w1 = new Text();
        private Text w2 = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("got from record reader the line " + value);
            String[] bdosLine= value.toString().split("\\R"); // bigram TAB year TAB occurrences TAB books
            Iterator<String> bdoIterator = Arrays.stream(bdosLine).iterator();
            String bdoLine;
            IntWritable occurrences;
            while (bdoIterator.hasNext()) {
                bdoLine = bdoIterator.next();
                logger.info("processing line " + bdoLine);
                String[] lineElements = bdoLine.split("\\t");
                logger.info("splitted line %s into: " + Arrays.toString(lineElements));
                BigramDecade bigramDecade = BigramDecade.fromString(lineElements[0]);
                logger.info("the bigram is " + bigramDecade);
                try {
                    occurrences = new IntWritable(Integer.parseInt((lineElements[1])));
                    logger.info("the bd is: " + bigramDecade);
                    logger.info("occurrences: " + occurrences.get());
                } catch (NumberFormatException ignored) {
                    continue;
                }
                context.write(new BigramDecadeOccurrences(bigramDecade, occurrences), one);
            }
        }
    }

    public static class BigramOccurrencesReducer extends Reducer<BigramDecadeOccurrences, IntWritable, BigramDecadeOccurrences, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void run(Reducer<BigramDecadeOccurrences, IntWritable, BigramDecadeOccurrences, IntWritable>.Context context) throws IOException, InterruptedException {
            this.setup(context);
            int count = 0;
            try {
                while(context.nextKey() && count++ <= 100) {
                    this.reduce(context.getCurrentKey(), context.getValues(), context);
                    Iterator<IntWritable> iter = context.getValues().iterator();
                    if (iter instanceof ReduceContext.ValueIterator) {
                        ((ReduceContext.ValueIterator)iter).resetBackupStore();
                    }
                }
            } finally {
                this.cleanup(context);
            }
        }

        @Override
        public void reduce(BigramDecadeOccurrences key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, one);
//            int takes = 0;
//            Iterator<BigramDecadeOccurrences> iter = values.iterator();
//            BigramDecadeOccurrences curr_bdo = iter.next();
//            IntWritable currentDecade = curr_bdo.getBigramDecade().getDecade();
//            while (iter.hasNext()) {
//                while (iter.hasNext() && curr_bdo.getBigramDecade().getDecade() == currentDecade && takes++ <= 100){
//                    context.write(key, one);
//                    curr_bdo = iter.next();
//                }
//                while (currentDecade == curr_bdo.getBigramDecade().getDecade())
//                    curr_bdo = iter.next();
//                takes = 0;
//            }
        }

    }

    public static void main(String[] args) throws Exception {
        logger.info("Starting " + step2SortBigramsDecadeByOccurrence.class.getName() + " map reduce app");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step2SortBigramsDecadeByOccurrence.class);
        job.setMapperClass(BigramOccurrencesMapper.class);
        job.setMapOutputKeyClass(BigramDecadeOccurrences.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(BigramOccurrencesReducer.class);
        job.setReducerClass(BigramOccurrencesReducer.class);
        job.setOutputKeyClass(BigramDecadeOccurrences.class);
        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-assignment-2/output" + System.currentTimeMillis()));
        FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/output" + System.currentTimeMillis()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        logger.info("Finished job 2 Successfully\n");
    }
}
