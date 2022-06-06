package com.dsp.mr_app;

import com.dsp.models.Unigram;
import com.dsp.models.UnigramDecade;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class step1UnigramCount {

    public static final Logger logger = Logger.getLogger(step1UnigramCount.class);
    public static final String BUCKET_HOME_SCHEME = "s3://dsp-assignment-2/";

    public static class UnigramMapper extends Mapper<Object, Text, UnigramDecade, IntWritable> {
        enum CountersEnum {
            INPUT_WORDS,
            SKIPPED_WORDS
        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("key: " + key);
            logger.info("value: " + value);
            String[] unigramLines = value.toString().split("\\R");
            Iterator<String> unigramItertor = Arrays.stream(unigramLines).iterator();
            String unigramLine;
            int year;
            Unigram unigram;
            IntWritable count;
            while (unigramItertor.hasNext()) {
                context.getCounter(CountersEnum.INPUT_WORDS).increment(1);
                unigramLine = unigramItertor.next();
                logger.info("processing line " + unigramLine);
                String[] lineElements = unigramLine.split("\\t");
                try {
                    unigram = (caseSensitive) ? Unigram.fromString(lineElements[0]) : Unigram.fromString(lineElements[0].toLowerCase());
                    if (patternsToSkip.contains(unigram.toString())) {
                        context.getCounter(CountersEnum.SKIPPED_WORDS).increment(1);
                        continue;
                    }
                    year = Integer.parseInt(lineElements[1]);
                    count = new IntWritable(Integer.parseInt((lineElements[2])));
                } catch (NumberFormatException ignored) {
                    continue;
                }
                IntWritable decade = new IntWritable(year / 10);
                logger.info("writing unigram '" + unigram + "', decade: " + decade + ", count: " + count);
                context.write(new UnigramDecade(unigram, decade), count);
                context.write(new UnigramDecade(Unigram.fromString("*"), decade), count); // for counting total words per decade
            }
        }
    }

    public static class UnigramPartitioner extends Partitioner<UnigramDecade, IntWritable> {

        @Override
        public int getPartition(UnigramDecade unigramDecade, IntWritable intWritable, int i) {
            return (unigramDecade.getUnigram().toString().equals("*")) ? 1 : 0;
        }
    }

    public static class UnigramDecadeCombiner extends Reducer<UnigramDecade, IntWritable, UnigramDecade, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(UnigramDecade key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            logger.info("starting to count occurrences for " + key);
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            logger.info("counted: " + sum);
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class UnigramDecadeReducer extends Reducer<UnigramDecade, IntWritable, UnigramDecade, Text> {

        private int total = 1;
        private int currentDecade = 0;

        public void reduce(UnigramDecade key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            logger.info("starting to count occurrences for " + key);
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (new Text("*").equals(key.getUnigram().getUnigram())) {
                total = sum;
                currentDecade = key.getDecade().get();
            } else {
                // This means we have a new decade without count
                if(key.getDecade().get() != currentDecade){
                    total = 1;
                    currentDecade = key.getDecade().get();
                }
                logger.info("counted: " + sum);
                context.write(key, new Text(String.format("%d,%d", total, sum)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step1UnigramCount.class);
        job.setMapperClass(UnigramMapper.class);
        //job.setPartitionerClass(UnigramPartitioner.class);
        job.setCombinerClass(UnigramDecadeCombiner.class);
        job.setReducerClass(UnigramDecadeReducer.class);
        //job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(UnigramDecade.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(UnigramDecade.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        job.addCacheFile(new Path("/home/hadoop/stop-words/eng-stopwords.txt").toUri());
        job.addCacheFile(new Path("/home/hadoop/stop-words/heb-stopwords.txt").toUri());
//        job.addCacheFile(new URI(BUCKET_HOME_SCHEME + "stop-words/eng-stopwords.txt"));
//        job.addCacheFile(new URI(BUCKET_HOME_SCHEME + "stop-words/heb-stopwords.txt"));
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        SequenceFileInputFormat.setInputPaths(job, new Path("/home/hadoop/google-1grams/data"));
//        SequenceFileInputFormat.setInputPaths(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data"));

        FileInputFormat.addInputPath(job, new Path("/home/hadoop/google-1grams/1grams-sample.txt"));
        args[0] = "/home/hadoop/outputs/output" + System.currentTimeMillis();
//        args[0] = BUCKET_HOME_SCHEME + "outputs/output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        int done = job.waitForCompletion(true) ? 0 : 1;
        if (done == 1)
            System.exit(1);
    }
}