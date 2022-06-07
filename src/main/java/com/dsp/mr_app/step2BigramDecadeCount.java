package com.dsp.mr_app;

import com.dsp.dsp_assignment_2.PathEnum;
import com.dsp.models.Bigram;
import com.dsp.models.BigramDecade;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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
import java.util.stream.Collectors;

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
                patternsToSkip = caseSensitive ? patternsToSkip : patternsToSkip.stream().map(String::toLowerCase).collect(Collectors.toSet());
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

            while (bigramItertor.hasNext()) {
                bigramLine = bigramItertor.next();
                logger.info("processing line " + bigramLine);
                String[] lineElements = bigramLine.split("\\t");
                String bigramStr = (caseSensitive) ? lineElements[0] : lineElements[0].toLowerCase();
                if(bigramStr.split("\\s").length < 2)
                    continue;
                if (Arrays.stream(bigramStr.split("\\s")).anyMatch(patternsToSkip::contains)) {
                    logger.info("skipping line " + bigramLine);
                    context.getCounter(step2BigramDecadeCount.BigramMapper.CountersEnum.SKIPPED_WORDS).increment(1);
                    continue;
                }
                Bigram bigram = new Bigram(new Text(bigramStr.split("\\s")[0]), new Text(bigramStr.split("\\s")[1]));
                try {
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

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 1) {
            logger.error("not place to store output path");
            System.exit(1);
        }
        logger.info("Starting " + step2BigramDecadeCount.class.getName() + " map reduce app");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        job.setJarByClass(step2BigramDecadeCount.class);
        job.setMapperClass(BigramMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(BigramDecade.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(BigramDecade.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new Path(args[PathEnum.STOP_WORDS.value]).toUri());
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[PathEnum.BIGRAMS.value]));
        FileOutputFormat.setOutputPath(job, new Path(args[PathEnum.STEP_2_OUTPUT.value]));
        int done = job.waitForCompletion(true) ? 0 : 1;
        if(done == 1)
            System.exit(1);
    }
}
