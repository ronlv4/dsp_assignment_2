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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import java.io.IOException;

public class step3MergeUnigramsBigramsLeft {

    public static final Logger logger = Logger.getLogger(step3MergeUnigramsBigramsLeft.class);


    public static class MergeMapper extends Mapper<Object, Text, BigramDecade, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\\t");
            String val =keyValue[1];
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
            context.write(newKey, new Text(val));
        }
    }

    public static class MergePartitioner extends Partitioner<BigramDecade, Text> {

        @Override
        public int getPartition(BigramDecade bigramDecade, Text text, int numPartitions) {
            return (bigramDecade.getBigram().getFirst().toString().hashCode() & 0x7fffffff) % numPartitions;
        }
    }


    public static class MergeReducer extends Reducer<BigramDecade, Text, BigramDecade, Text> {
        String[] currentTotal = {"1", "1"};
        int currentDecade = 0;
        String currentLeftWord = "";
        public void reduce(BigramDecade key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text val = values.iterator().next();
            if(key.getBigram().getSecond().equals(new Text("*"))) {
                currentTotal[0] = val.toString().split(",")[0];
                currentTotal[1] = val.toString().split(",")[1];
                currentDecade = key.getDecade().get();
                currentLeftWord = key.getBigram().getFirst().toString();
            }
            else {
                if(currentDecade != key.getDecade().get()) {
                    currentTotal[0] = "1";
                    currentDecade = key.getDecade().get();
                }
                if(!currentLeftWord.equals(key.getBigram().getFirst().toString())) {
                    currentTotal[1] = "1";
                    currentLeftWord = key.getBigram().getFirst().toString();
                }
                String concated = String.format("%s,%s", val, String.join(",", currentTotal));
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
        job.setJarByClass(step3MergeUnigramsBigramsLeft.class);
        job.setMapperClass(MergeMapper.class);
        job.setPartitionerClass(MergePartitioner.class);
        job.setReducerClass(MergeReducer.class);
        job.setMapOutputKeyClass(BigramDecade.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(BigramDecade.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[PathEnum.STEP_1_OUTPUT.value]));
        FileInputFormat.addInputPath(job, new Path(args[PathEnum.STEP_2_OUTPUT.value]));
        FileOutputFormat.setOutputPath(job, new Path(args[PathEnum.STEP_3_OUTPUT.value]));
        int done = job.waitForCompletion(true) ? 0 : 1;
        if(done == 1)
            System.exit(1);
    }
}
