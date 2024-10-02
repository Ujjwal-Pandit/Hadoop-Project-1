package org.ujjwal;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_3 extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line
        String[] fields = value.toString().split(",");
        // Assuming fields[0] is ID and fields[1] is Nickname
        if (fields.length >= 2) {
            String id = fields[0].trim();
            String nickname = fields[1].trim();
            context.write(new Text(id), new Text(nickname));
        }
    }
}