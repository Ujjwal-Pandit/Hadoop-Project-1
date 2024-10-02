package org.ujjwal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class CountReducer extends Reducer<Text, IntWritable, Text, Text> {
    private ArrayList<CountPair> countList = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Sum up the counts for each key
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Add the count and key to the list
        countList.add(new CountPair(sum, key.toString()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Sort the list by count in descending order
        Collections.sort(countList, new Comparator<CountPair>() {
            @Override
            public int compare(CountPair o1, CountPair o2) {
                return Integer.compare(o2.count, o1.count); // Descending order
            }
        });

        // Emit only the top 10 keys with a placeholder value
        for (int i = 0; i < Math.min(10, countList.size()); i++) {
            CountPair pair = countList.get(i);
            context.write(new Text(pair.key), new Text("")); // Emit key with a placeholder
        }
    }

    // Helper class to hold count and key
    private static class CountPair {
        int count;
        String key;

        CountPair(int count, String key) {
            this.count = count;
            this.key = key;
        }
    }
}