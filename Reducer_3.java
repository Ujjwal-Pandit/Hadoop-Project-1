package org.ujjwal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer_3 extends Reducer<LongWritable, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Check for matches between ComparisonReducer output and Mapper_3 output
        for (Text value : values) {
            // Emit the key and value (name) if there is a match
            context.write(key, value); // Emit (id, name)
        }
    }
}