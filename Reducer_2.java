package org.ujjwal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class  Reducer_2 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> uniquePartners = new HashSet<>();

        // Collect unique partners for the key
        for (Text value : values) {
            uniquePartners.add(value.toString());
        }

        // Emit pairs in both directions
        for (String partner : uniquePartners) {
            // Emit Id1 -> Id2
            context.write(key, new Text(partner));

        }
    }
}
