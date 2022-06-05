package com.dsp.mr_app;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class step3MergeUnigramsBigrams {

    public static final Logger logger = Logger.getLogger(step2BigramDecadeCount.class);
    public static final String BUCKET_HOME_SCHEME = "s3://dsp-assignment-2/";


    public static class MergeMapper extends Mapper<Object, Text, BigramDecade, IntWritable> {
        private final HashMap<String, Integer> wordsPerDecade = new HashMap<>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
//            conf = context.getConfiguration();
//            URI[] wordsPerDecade = Job.getInstance(conf).getCacheFiles();
//            parseWordsPerDecade(new Path(wordsPerDecade[0].getPath()).getName());
        }

        private void parseWordsPerDecade(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String line = null;
                while ((line = fis.readLine()) != null) {
                    int count = Integer.parseInt(line.split("\\t")[1]);
                    String decade = line.split("\\t")[0].split(":")[1];
                    wordsPerDecade.put(decade, count);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("got from record reader the line " + value);
            String[] keyValue = value.toString().split("\\t");
            int val = Integer.parseInt(keyValue[1]);
            String[] bigramOrUnigramDecade = keyValue[0].split(":");
            int decade = Integer.parseInt(bigramOrUnigramDecade[1]);
            String[] words = bigramOrUnigramDecade[0].split("\\s");
            Bigram b;
            if(words.length > 1) {
                b = new Bigram(new Text(words[0]), new Text(words[1]));

            }
            else {
                b = new Bigram(new Text(words[0]), new Text("*"));

            }
            BigramDecade newKey = new BigramDecade(b, new IntWritable(decade));
            context.write(newKey, new IntWritable(val));
        }
    }


    public static class IntSumReducer extends Reducer<BigramDecade, IntWritable, BigramDecade, Text> {
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
        logger.info("Starting " + step3MergeUnigramsBigrams.class.getName() + " map reduce app");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step3MergeUnigramsBigrams.class);
        job.setMapperClass(MergeMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(BigramDecade.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(BigramDecade.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/outputs/output" + System.currentTimeMillis()));
        int done = job.waitForCompletion(true) ? 0 : 1;
        if(done == 1)
            System.exit(1);
    }
}
