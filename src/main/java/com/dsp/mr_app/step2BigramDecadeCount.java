package com.dsp.mr_app;

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
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class step2BigramDecadeCount {
    /*
    step1BigramDecadeCount Input:
        Key: line number of lzo file
        Value: w1 w2 TAB year TAB occurrences TAB booksRefs

    step1BigramDecadeCount Output:
        Key: <w1 w2:decade>
        Value: occurrences of the bigram <w1 w2> in the decade
     */

    public static final Logger logger = Logger.getLogger(step2BigramDecadeCount.class);
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

        enum CountersEnum {
            INPUT_WORDS,
            SKIPPED_WORDS
        }
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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("got from record reader the line " + value);
            String[] bigramLines = value.toString().split("\\R"); // bigram TAB year TAB occurrences TAB books
            Iterator<String> bigramItertor = Arrays.stream(bigramLines).iterator();
            String bigramLine;
            int year;
            IntWritable count;
            Text bigram;
            while (bigramItertor.hasNext()) {
                bigramLine = bigramItertor.next();
                logger.info("processing line " + bigramLine);
                String[] lineElements = bigramLine.split("\\t");
                try {
                    bigram = (caseSensitive) ? new Text(lineElements[0]) : new Text(lineElements[0].toLowerCase());
                    if (Arrays.stream(bigram.toString().split("\\s")).anyMatch(patternsToSkip::contains)) {
                        logger.info("skipping line " + bigramLine);
                        context.getCounter(step2BigramDecadeCount.BigramMapper.CountersEnum.SKIPPED_WORDS).increment(1);
                        continue;
                    }
                    year = Integer.parseInt(lineElements[1]);
                    count = new IntWritable(Integer.parseInt((lineElements[2])));
                } catch (NumberFormatException ignored) {
                    continue;
                }
                IntWritable decade = new IntWritable(year / 10);
                logger.info("writing bigram '" + bigram + "', decade: " + decade + ", count: " + count);
                context.write(new BigramDecade(bigram, decade), count);
            }
        }
    }

    public static class IntSumReducer extends Reducer<BigramDecade, IntWritable, BigramDecade, IntWritable> {
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

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        if (args.length < 1) {
            logger.error("not place to store output path");
            System.exit(1);
        }
        logger.info("Starting " + step2BigramDecadeCount.class.getName() + " map reduce app");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step2BigramDecadeCount.class);
        job.setMapperClass(BigramMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(BigramDecade.class);
        job.setOutputValueClass(IntWritable.class);
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
//        job.addCacheFile(new Path("/home/hadoop/stop-words/eng-stopwords.txt").toUri());
//        job.addCacheFile(new Path("/home/hadoop/stop-words/heb-stopwords.txt").toUri());
        job.addCacheFile(new URI(BUCKET_HOME_SCHEME + "stop-words/eng-stopwords.txt"));
        job.addCacheFile(new URI(BUCKET_HOME_SCHEME + "stop-words/heb-stopwords.txt"));

        //FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data"));
        FileInputFormat.addInputPath(job, new Path(BUCKET_HOME_SCHEME + "google-2grams/"));
//        FileInputFormat.addInputPath(job, new Path("/home/hadoop/input/"));
        //FileOutputFormat.setOutputPath(job, new Path("s3://dsp-assignment-2/output" + System.currentTimeMillis()));
//        args[0] = "/home/hadoop/output" + System.currentTimeMillis();
        args[0] = BUCKET_HOME_SCHEME + "output" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.waitForCompletion(true);
        logger.info("Finished job 1 Successfully\n");
    }
}
