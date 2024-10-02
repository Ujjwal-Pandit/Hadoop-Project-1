package org.ujjwal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper_1 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] ids = line.split(",");

        if (ids.length == 5) {
            String id1 = ids[1].trim();
            String id2 = ids[2].trim();

            // Emit Id1 -> Id2
            context.write(new Text(id1), new Text(id2));

            // Emit Id2 -> Id1
            context.write(new Text(id2), new Text(id1));
        }
    }
}