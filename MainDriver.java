package org.ujjwal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class MainDriver {
    public static void main(String[] args) throws Exception {
        // Check for proper input and output paths
        if (args.length != 7) {
            System.err.println("Usage: MainDriver <output path for Reducer_1> <input path for Mapper_1> " +
                    "<output path for Reducer_2> <input path for Mapper_2> <output path for Comparison Job> " +
                    "<input path for Mapper_3> <output path for final results>");
            System.exit(-1);
        }

        // Job 1: Mapper_1 and Reducer_1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job 1: Mapper_1 and Reducer_1");
        job1.setJarByClass(MainDriver.class);
        job1.setMapperClass(Mapper_1.class);

        job1.setCombinerClass(Reducer_1.class);
        job1.setReducerClass(Reducer_1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[1])); // Input for Mapper_1
        FileOutputFormat.setOutputPath(job1, new Path(args[0])); // Output for Reducer_1
        boolean success1 = job1.waitForCompletion(true);
        if (!success1) {
            System.exit(1);
        }

        // Job 2: Mapper_2 and Reducer_2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Job 2: Mapper_2 and Reducer_2");
        job2.setJarByClass(MainDriver.class);
        job2.setMapperClass(Mapper_2.class);
        job2.setCombinerClass(Reducer_2.class);
        job2.setReducerClass(Reducer_2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[3])); // Input for Mapper_2
        FileOutputFormat.setOutputPath(job2, new Path(args[2])); // Output for Reducer_2
        boolean success2 = job2.waitForCompletion(true);
        if (!success2) {
            System.exit(1);
        }

        // Job 3: Comparison Job
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Job 3: Comparison Job");
        job3.setJarByClass(MainDriver.class);
        job3.setMapperClass(ComparisonMapper.class);
        job3.setCombinerClass(ComparisonReducer.class);// Use the custom mapper
        job3.setReducerClass(ComparisonReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job3, new Path(args[0]), TextInputFormat.class, ComparisonMapper.class);
        MultipleInputs.addInputPath(job3, new Path(args[2]), TextInputFormat.class, ComparisonMapper.class);
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));

// Execute Job 3
        boolean success3 = job3.waitForCompletion(true);
        if (!success3) {
            System.exit(1);
        }

        // Job 4: Mapper_3 and Reducer_3
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "Job 4: Mapper_3 and Reducer_3");
        job4.setJarByClass(MainDriver.class);
        job4.setMapperClass(Mapper_3.class);
        job4.setCombinerClass(Reducer_3.class);// Mapper for the third CSV
        job4.setReducerClass(Reducer_3.class); // Reducer for final output
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        // Input for Mapper_3
        FileInputFormat.addInputPath(job4, new Path(args[5])); // Input for Mapper_3
        FileOutputFormat.setOutputPath(job4, new Path(args[6])); // Output for final results

        // Execute Job 4
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}