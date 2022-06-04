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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class step2SortBigramsDecadeByOccurrence {

    /*
    step2SortBigramsDecadeByOccurrence Input:
        Key: line number
        Value: <w1 w2:decade> occurrences

    step2SortBigramsDecadeByOccurrence Output:
        Key: <w1 w2:decade:occurrences>
        Value: one
        Sort by: decade > occurrences > w1 w2
        Limit: 100 Per decade
     */

    public static final Logger logger = Logger.getLogger(step2SortBigramsDecadeByOccurrence.class);
    public static final int MAX_BIGRAMS = 5;
    public static final String BUCKET_HOME_SCHEME = "s3://dsp-assignment-2/";
    private static IntWritable one = new IntWritable(1);

    public static class BigramOccurrencesMapper extends Mapper<Object, Text, BigramDecadeOccurrences, IntWritable> {
        /*
        Mapper Input:
            Key: line number
            Value: <w1 w2:decade> occurrences
            Where:
                w1 w2 is the bigram
                decade is the decade of the bigram
                occurrences is the number of occurrences of the bigram in the decade
        Mapper Output:
            Key: <w1 w2:decade:occurrences>
            Value: one
         */

        private Text w1 = new Text();
        private Text w2 = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("got from record reader the line " + value);
            String[] bdosLines= value.toString().split("\\R"); // TODO: check if we already get single line
            Iterator<String> bdoIterator = Arrays.stream(bdosLines).iterator();
            String bdoLine;
            IntWritable occurrences;
            BigramDecade bigramDecade;
            while (bdoIterator.hasNext()) {
                bdoLine = bdoIterator.next();
                logger.info("processing line " + bdoLine);
                String[] lineElements = bdoLine.split("\\t");
                try {
                    bigramDecade = BigramDecade.fromString(lineElements[0]);
                    occurrences = new IntWritable(Integer.parseInt((lineElements[1])));
                } catch (NumberFormatException ignored) {
                    continue;
                }
//                context.write(bigramDecade.getDecade(), new BigramDecadeOccurrences(bigramDecade, occurrences));
                logger.info("writing bigram " + bigramDecade + ", occurrences: " + occurrences);
                context.write(new BigramDecadeOccurrences(bigramDecade, occurrences), one); //bigram:200:2560  1
            }
        }
    }

    public static class BigramOccurrencesReducer extends Reducer<BigramDecadeOccurrences, IntWritable, BigramDecadeOccurrences, IntWritable> {

/*
Reducer Input: same as mapper output
    Key: <w1 w2:decade:occurrences>
    Value: Iterable<one>
    Sort by: decade > occurrences > w1 w2
Reducer Output:
    Key: <w1 w2:decade:occurrences>
    Value: one
    Sort by: decade > occurrences > w1 w2
    Limit: 100 Per decade
 */

        private IntWritable result = new IntWritable();
        private static AtomicInteger takes = new AtomicInteger(0);
        private static AtomicInteger currentDecade = new AtomicInteger(0);
        private static ConcurrentHashMap<Integer, Integer> decadeCountMap = new ConcurrentHashMap<>();

//        @Override
//        public void run(Reducer<BigramDecadeOccurrences, IntWritable, BigramDecadeOccurrences, IntWritable>.Context context) throws IOException, InterruptedException {
//            this.setup(context);
//            logger.info("inside run function");
//            logger.info("context current key: " + context.getCurrentKey());
//            int takes = 0;
//            IntWritable currentDecade;
//            BigramDecadeOccurrences prev;
//            try {
//                context.nextKey();
//                while(context.getCurrentKey() != null) {
//                    prev = context.getCurrentKey();
//                    logger.info("context current key: " + context.getCurrentKey());
//                    currentDecade = context.getCurrentKey().getBigramDecade().getDecade();
//                    logger.info("current decade: " + currentDecade);
//                    while (context.getCurrentKey().getBigramDecade().getDecade() == currentDecade && takes++ <= 100 && context.nextKeyValue()){
//                        logger.info("calling reduce with bdo " + prev);
//                        this.reduce(prev, context.getValues(), context);
//                        prev = context.getCurrentKey();
//                        logger.info("reassigning prev to " + prev);
//                    }
//                    if (takes >= 101) {
//                        logger.info("takes are " + takes);
//                        while (context.nextKey() && context.getCurrentKey().getBigramDecade().getDecade() == currentDecade) {
//                            logger.info("advancing through bdo " + context.getCurrentKey());
//                        }
//                        logger.info("finished advancing beyond current decade");
//
//                    }
//                    takes = 0;
//                    Iterator<IntWritable> iter = context.getValues().iterator();
//                    if (iter instanceof ReduceContext.ValueIterator) {
//                        ((ReduceContext.ValueIterator)iter).resetBackupStore();
//                    }
//                }
//            } finally {
//                this.cleanup(context);
//            }
//        }

        @Override
        public synchronized void reduce(BigramDecadeOccurrences key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            logger.info("got bdo " + key);
            currentDecade.set(key.getBigramDecade().getDecade().get());
            if (decadeCountMap.getOrDefault(currentDecade.get(), 0) < MAX_BIGRAMS){
                decadeCountMap.compute(currentDecade.get(), (decade, count) -> count == null ? 1 : count + 1);
                context.write(key, one);
            }
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
//        job.setCombinerClass(BigramOccurrencesReducer.class);
        job.setReducerClass(BigramOccurrencesReducer.class);
        job.setOutputKeyClass(BigramDecadeOccurrences.class);
        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data"));
//        FileInputFormat.addInputPath(job, new Path("s3://dsp-assignment-2/google-2grams/"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-assignment-2/output" + System.currentTimeMillis()));
        FileOutputFormat.setOutputPath(job, new Path(BUCKET_HOME_SCHEME+ "output" + System.currentTimeMillis()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        logger.info("Finished job 2 Successfully\n");
    }
}
