package org.ujjwal;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NameOcc_Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text name = new Text();
    private Text occ = new Text();
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Tokenize the input string
        String[] line = value.toString().split(",");
        String nickname = line[1];
        String occupation = line[2];
        String highestEdu = line[4];
        if (highestEdu.equals("MS")) {
            name.set(nickname);
            occ.set(occupation);
            context.write(name, occ);
        }
    }
}