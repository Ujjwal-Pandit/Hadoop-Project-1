package org.ujjwal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ClosingFuture;

public class Outdated_driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Outdated_driver <input_path> <intermediate_output> <linkbookpage_path> <final_output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // First Job Configuration - This runs Mapper_1 and Reducer_1
        Job job1 = Job.getInstance(conf, "Our Education Level Students");
        job1.setJarByClass(org.ujjwal.Outdated_driver.class);
        job1.setMapperClass(Mapper_1.class);
        job1.setCombinerClass(Reducer_1.class);
        job1.setReducerClass(Reducer_1.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Input and Output paths for the first job
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // Wait for the completion of the first job
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Second Job Configuration - This runs Mapper_2 and Reducer_2 for the join operation
        Job job2 = Job.getInstance(conf, "ID and Nickname Join");
        job2.setJarByClass(org.ujjwal.Outdated_driver.class);
        job2.setMapperClass(Mapper_2.class);
        job2.setMapperClass(Mapper_3.class);
        job2.setCombinerClass(FinalReducer.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // Set input paths for the second job - taking Reducer_1's output and the LinkBookPage file
        FileInputFormat.addInputPath(job2, new Path(args[1]));  // Output from Job 1 as input
        FileInputFormat.addInputPath(job2, new Path(args[2]));  // LinkBookPage file as input
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        // Wait for the completion of the second job
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}