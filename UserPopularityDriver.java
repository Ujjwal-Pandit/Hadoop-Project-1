package org.ujjwal;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserPopularityDriver {

    public static class AssociatesMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String personA_ID = fields[1];
            String personB_ID = fields[2];
            context.write(new Text(personA_ID), new Text("Asso" + "," + "1"));
            context.write(new Text(personB_ID), new Text("Asso" + "," + "1"));
        }
    }

    public static class LinkBookPageMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            String userID = fields[0];
            String userName = fields[1];
            context.write(new Text(userID), new Text("Link" + "," + userName));
        }
    }

    public static class ReduceJoinReducer
            extends Reducer<Text, Text, Text, Text> {

            double tot;
            int n;
            private Map<String, Integer> countMap;
            private Map<String, String> userMap;

            @Override
            protected void setup(Context context)
                    throws IOException, InterruptedException {
                tot = 0.0;
                n = 0;
                countMap = new HashMap<>();
                userMap = new HashMap<>();
            }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String userName = "";
            int count = 0;
            for (Text txt : values) {
                String parts[] = txt.toString().split(",");
                if (parts[0].equals("Asso")) {
                    count += Integer.parseInt(parts[1]);
                } else if (parts[0].equals("Link")) {
                    userName = parts[1];
                }
            }
            n += 1;
            tot += count;
            countMap.put(key.toString(), count);
            userMap.put(key.toString(), userName);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            double average = tot/ n;
            String average_val = String.format("%f", average);
            context.write(new Text("avg"), new Text(average_val));
            for (String userID : countMap.keySet()) {
                String userName = userMap.get(userID);
                Integer count = countMap.get(userID);
                if (count > average) {
                    context.write( new Text(userName ),new Text(String.valueOf(count)));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Avg Popularity");
        job.setJarByClass(UserPopularityDriver.class);
        job.setCombinerClass(ReduceJoinReducer.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, LinkBookPageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

    }
}