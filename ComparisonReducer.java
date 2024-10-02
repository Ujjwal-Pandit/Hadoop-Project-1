package org.ujjwal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ComparisonReducer extends Reducer<Text, Text, Text, Text> {
    private Map<Text, Text> reducer1Values = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Iterate through the values to separate Reducer_1 and Reducer_2 values
        boolean isFromReducer1 = true; // Assuming the first input is from Reducer_1

        for (Text value : values) {
            if (isFromReducer1) {
                // Store the value from Reducer_1
                reducer1Values.put(new Text(key), new Text(value));
            } else {
                // Compare with values from Reducer_1
                if (reducer1Values.containsKey(key)) {
                    Text valueFromReducer1 = reducer1Values.get(key);
                    if (!valueFromReducer1.toString().equals(value.toString())) {
                        // Emit (key, "") if values do not match
                        context.write(key, new Text(""));
                    }
                }
            }
            // Toggle the flag for the next call (you may need a better way to differentiate inputs)
            isFromReducer1 = false; // Change this logic based on your input format
        }
    }
}