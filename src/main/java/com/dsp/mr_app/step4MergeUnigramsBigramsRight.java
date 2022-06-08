package com.dsp.mr_app;

import com.dsp.dsp_assignment_2.PathEnum;
import com.dsp.models.ReverseBigram;
import com.dsp.models.ReverseBigramDecade;
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
import org.apache.log4j.Logger;
import java.io.IOException;

public class step4MergeUnigramsBigramsRight {

    public static final Logger logger = Logger.getLogger(step4MergeUnigramsBigramsRight.class);


    public static class MergeMapper extends Mapper<Object, Text, ReverseBigramDecade, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\\t");
            String val =keyValue[1];
            String[] bigramOrUnigramDecade = keyValue[0].split(":");
            int decade = Integer.parseInt(bigramOrUnigramDecade[1]);
            String[] words = bigramOrUnigramDecade[0].split("\\s");
            ReverseBigram b;
            if(words.length > 1) {
                b = new ReverseBigram(new Text(words[0]), new Text(words[1]));

            }
            else {
                b = new ReverseBigram(new Text("*"), new Text(words[0]));

            }
            ReverseBigramDecade newKey = new ReverseBigramDecade(b, new IntWritable(decade));
            context.write(newKey, new Text(val));
        }
    }

    public static class ReverseMergePartitioner extends Partitioner<ReverseBigramDecade, Text> {
        @Override
        public int getPartition(ReverseBigramDecade reverseBigramDecade, Text text, int numPartitions) {
            return (reverseBigramDecade.getBigram().getSecond().toString().hashCode() & 0x7fffffff) % numPartitions;
        }
    }


    public static class MergeReducer extends Reducer<ReverseBigramDecade, Text, ReverseBigramDecade, Text> {

        String[] currentTotal = {"1", "1"};

        int currentDecade = 0;
        String currentRightWord = "";

        public void reduce(ReverseBigramDecade key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text val = values.iterator().next();
            if(key.getBigram().getFirst().equals(new Text("*"))) {
                currentTotal[0] = val.toString().split(",")[0];
                currentTotal[1] = val.toString().split(",")[1];
                currentDecade = key.getDecade().get();
                currentRightWord = key.getBigram().getSecond().toString();
            }
            else {
                if(currentDecade != key.getDecade().get()) {
                    currentTotal[0] = "1";
                    currentDecade = key.getDecade().get();
                }
                if(!currentRightWord.equals(key.getBigram().getSecond().toString())) {
                    currentTotal[1] = "1";
                    currentRightWord = key.getBigram().getSecond().toString();
                }
                String concated = String.format("%s,%s", val, currentTotal[1]);
                context.write(key, new Text(concated));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 1) {
            logger.error("not place to store output path");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(step4MergeUnigramsBigramsRight.class);
        job.setMapperClass(MergeMapper.class);
        job.setPartitionerClass(ReverseMergePartitioner.class);
        job.setReducerClass(MergeReducer.class);
        job.setMapOutputKeyClass(ReverseBigramDecade.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(ReverseBigramDecade.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[PathEnum.STEP_1_OUTPUT.value]));
        FileInputFormat.addInputPath(job, new Path(args[PathEnum.STEP_3_OUTPUT.value]));
        FileOutputFormat.setOutputPath(job, new Path(args[PathEnum.STEP_4_OUTPUT.value]));
        int done = job.waitForCompletion(true) ? 0 : 1;
        if(done == 1)
            System.exit(1);
    }
}
