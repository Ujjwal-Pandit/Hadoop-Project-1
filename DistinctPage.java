package org.ujjwal;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistinctPage {

    public static class DistinctMapper extends Mapper<Object, Text, IntWritable, Text> {

        private IntWritable userId = new IntWritable();
        private Text pageId = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

                String line = value.toString().trim();



                String[] fields = line.split(",");

                int id;
                int pageid;


                    id= Integer.parseInt(fields[1].trim());
                    pageid= Integer.parseInt(fields[2].trim());




                userId.set(id);
                pageId.set(String.valueOf(pageid));
                context.write(userId, pageId); // Emit (ByWho, WhatPage)

        }
    }

    public static class DistinctReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count= 0;
            HashSet<String> distPages = new HashSet<>();

            for (Text val : values) {
                count++;
                distPages.add(val.toString());
            }

            context.write(key, new Text(count + "\t" + distPages.size()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DistinctPage");

        job.setJarByClass(DistinctPage.class);
        job.setMapperClass(DistinctMapper.class);
        job.setCombinerClass(DistinctReducer.class);
        job.setReducerClass(DistinctReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}