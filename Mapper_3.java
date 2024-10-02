package org.ujjwal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper_3 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        if (fields.length == 5) { // Assuming ID and Name are the first two fields
            String id = fields[0].trim();
            String name = fields[1].trim();
            context.write(new Text(id), new Text(name)); // Emit (id, name)
        }
    }
}
