package org.ujjwal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class Final_Reducer extends Reducer<Text, Text, Text, Text> {
    private Set<String> topIds = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        // Read the top 10 IDs from job1's output
        Path intermediateOutputPath = new Path(conf.get("intermediate.output.path"));
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(intermediateOutputPath)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length > 0) {
                    topIds.add(parts[0].trim());
                }
            }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String id = key.toString();

        // Check if this ID is in the top 10 from job1
        if (topIds.contains(id)) {
            for (Text value : values) {
                String[] details = value.toString().split(",");
                if (details.length == 2) {
                    String nickname = details[0];
                    String occupation = details[1];

                    // Emit the details for the matching ID
                    context.write(new Text(id), new Text(nickname + "," + occupation));
                }
            }
        }
    }
}