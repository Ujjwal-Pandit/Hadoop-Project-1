package org.ujjwal;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper_2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by comma
        String[] fields = value.toString().split(",");

        if (fields.length == 5) {
            String id = fields[0].trim();           // ID
            String nickname = fields[1].trim();     // NickName
            String occupation = fields[2].trim();   // Occupation

            // Emitting (ID, (nickname, occupation))
            context.write(new Text(id), new Text(nickname + "," + occupation));
        }
    }
}