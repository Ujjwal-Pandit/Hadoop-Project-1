package org.ujjwal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {
        // Job 1: Mapper1 and CountReducer
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Top 10 ID Selection");

        job1.setJarByClass(Driver.class);
        job1.setMapperClass(Mapper_1.class);
        job1.setCombinerClass(CountReducer.class);
        job1.setReducerClass(CountReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);  // Changed to Text as per CountReducer output

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0])); // First CSV input path
        Path intermediateOutput = new Path(args[1]); // Intermediate output path for top 10 results
        FileOutputFormat.setOutputPath(job1, intermediateOutput);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2: Mapper2 and Final Reducer
        Configuration conf2 = new Configuration();
        conf2.set("intermediate.output.path", new Path(intermediateOutput, "part-r-00000").toString());

        Job job2 = Job.getInstance(conf2, "Matching and Final Output");

        job2.setJarByClass(Driver.class);
        job2.setMapperClass(Mapper_2.class);
        job2.setCombinerClass(Final_Reducer.class);
        job2.setReducerClass(Final_Reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // LinkBookPage file as input
        FileInputFormat.addInputPath(job2, new Path(args[2]));

        // Final output path
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}