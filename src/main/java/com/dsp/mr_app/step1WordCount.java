package com.dsp.mr_app;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class myWordCount {
    /**
     * Input to the mapper:
     * Key: Line number (not important).
     * Value: (2-gram - the actual words,
     *        year of this aggregation,
     *        occurrences in this year,
     *        pages - The number of pages this 2-gram appeared on in this year,
     *        books - The number of books this 2-gram appeared in during this year)
     *
     * Output of Mapper:
     *        Key: The word.
     *        Value: The amount of times it appeares in the year of this record.
     *
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] vals = value.toString().split(" ");
            String[] words = vals[0].split(" ");
            if(words.length>1){
                String w1 = words[0];
                String w2 = words[1];
                Text text = new Text(String.format("%s %s",w1,w2));
                Text occurrences = new Text(vals[2]);
                context.write(text ,occurrences);
            }
        }
    }


    /**
     * Input:
     *        Output of mapper.
     *
     * Output:
     *        Key: a pair of words (separated by a whitespace).
     *        Value: The total amount of times it appears in the corpus.
     *
     *        Notice that this is practically word-count.
     *
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumOccurrences = 0;
            for (Text occ : values) {
                sumOccurrences += Long.parseLong(occ.toString());
            }

            Text newVal = new Text(String.format("%d",sumOccurrences));
            // We send the same key, with the total amount of it's appearences in the corpus.
            context.write(key, newVal);
        }
    }

    private static class myPartitioner extends Partitioner<Text, Text>{
        @Override
        public int getPartition(Text key, Text value, int numPartitions){
            return Math.abs(key.hashCode()) % numPartitions;
        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting myWordCount\n");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2gram");
        job.setJarByClass(myWordCount.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(myPartitioner.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path("s3://dsp-assignment-2/input"));
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-assignment-2/output")); // created by aws
        job.waitForCompletion(true);
        System.out.println("Finished job Successfully\n");
    }
}
