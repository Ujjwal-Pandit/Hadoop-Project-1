package org.ujjwal;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String nickname = null;

        // Check for values from Mapper_3 (nickname)
        for (Text value : values) {
            if (!value.toString().isEmpty()) {
                nickname = value.toString(); // Store the nickname
            }
        }

        // If a nickname was found, output the (ID, Nickname) pair
        if (nickname != null) {
            context.write(key, new Text(nickname));
        }
    }
}