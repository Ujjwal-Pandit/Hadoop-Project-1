package org.ujjwal;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_2 extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Output from Reducer_1: single ID
        String id = value.toString().trim();
        // Emit ID with a placeholder value
        context.write(new Text(id), new Text("")); // Placeholder value
    }
}