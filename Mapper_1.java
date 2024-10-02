package org.ujjwal;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_1 extends Mapper<LongWritable, Text, IntWritable, Text> {

  //  private Text  = new Text();
  //  private Text occ = new Text();
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Tokenize the input string
        String[] line = value.toString().split(",");
        int id = Integer.parseInt(line[1]);
        int time = Integer.parseInt(line[4]);
        if (time > 129600) {

            context.write(new IntWritable(id),new Text("one"));
        }
    }
}