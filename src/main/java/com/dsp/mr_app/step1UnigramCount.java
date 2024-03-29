package com.dsp.mr_app;

import com.dsp.dsp_assignment_2.PathEnum;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

public class step1UnigramCount {

    public static final Logger logger = Logger.getLogger(step1UnigramCount.class);

    public static class UnigramMapper extends Mapper<Object, Text, UnigramDecade, IntWritable> {
        enum CountersEnum {
            INPUT_WORDS,
            SKIPPED_WORDS
        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;

        private boolean isEng;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            isEng = conf.getBoolean("is.eng", true);
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

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String unigramLine = value.toString();
            int year;
            Unigram unigram;
            String unigramString;
            IntWritable count;
            try{
                context.getCounter(CountersEnum.INPUT_WORDS).increment(1);
                String[] lineElements = unigramLine.split("\\t");
                unigramString = isEng ? lineElements[0].replaceAll("[^a-zA-Z]", "") : lineElements[0];
                unigram = (caseSensitive) ? Unigram.fromString(unigramString) : Unigram.fromString(unigramString.toLowerCase());
                if (patternsToSkip.contains(unigram.toString()) || unigram.toString().isEmpty()) {
                    context.getCounter(CountersEnum.SKIPPED_WORDS).increment(1);
                    return;
                }
                year = Integer.parseInt(lineElements[1]);
                    count = new IntWritable(Integer.parseInt((lineElements[2])));
                IntWritable decade = new IntWritable(year / 10);
                context.write(new UnigramDecade(unigram, decade), count);
                context.write(new UnigramDecade(Unigram.fromString("*"), decade), count); // for counting total words per decade
            }
            catch(Exception e){
                logger.info(String.format("Failed on %s with %s", value.toString(), e.getMessage()));
            }
        }
    }

    public static class UnigramDecadeCombiner extends Reducer<UnigramDecade, IntWritable, UnigramDecade, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(UnigramDecade key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class UnigramPartitioner extends Partitioner<UnigramDecade, IntWritable> {

        @Override
        public int getPartition(UnigramDecade unigramDecade, IntWritable intWritable, int i) {
            return unigramDecade.getDecade().get() % i;
        }
    }

    public static class UnigramDecadeReducer extends Reducer<UnigramDecade, IntWritable, UnigramDecade, Text> {

        private int total = 1;
        private int currentDecade = 0;

        public void reduce(UnigramDecade key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
                context.write(key, new Text(String.format("%d,%d", total, sum)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step1UnigramCount.class);
        job.setMapperClass(UnigramMapper.class);
        job.setPartitionerClass(UnigramPartitioner.class);
        job.setCombinerClass(UnigramDecadeCombiner.class);
        job.setReducerClass(UnigramDecadeReducer.class);
        job.setMapOutputKeyClass(UnigramDecade.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(UnigramDecade.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
        if(args[PathEnum.LANG.value].equals("heb"))
            job.getConfiguration().setBoolean("is.eng", false);
        job.addCacheFile(new Path(args[PathEnum.STOP_WORDS.value]).toUri());

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[PathEnum.UNIGRAMS.value]));
        FileOutputFormat.setOutputPath(job, new Path(args[PathEnum.STEP_1_OUTPUT.value]));
        int done = job.waitForCompletion(true) ? 0 : 1;
        if (done == 1)
            System.exit(1);
    }
}