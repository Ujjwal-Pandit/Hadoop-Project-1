package org.ujjwal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper_1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text id = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by comma (assuming CSV format)
        String[] fields = value.toString().split(",");

        if (fields.length ==5) {
            String idValue = fields[2].trim();


            // Emit the ID and associated ID for counting
            id.set(idValue ); // Creating a composite key
            context.write(id, one);
        }
    }
}