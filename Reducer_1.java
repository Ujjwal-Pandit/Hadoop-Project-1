package org.ujjwal;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_1 extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;

        // Count the number of occurrences for each key
        for (Text value : values) {
            count++;
        }

        // Write the key and the total count as output
        context.write(key, new IntWritable(count));
    }
}