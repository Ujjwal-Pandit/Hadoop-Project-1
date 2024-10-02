package org.ujjwal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HappyCount{

    // Mapper for LinkBookPage to extract id and nickname
    public static class LinkBookPageMapper extends Mapper<Object, Text, Text, Text> {
        private Text id = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                id.set(fields[0]);  // id
                String nickname = "nickname#" + fields[1];
                context.write(id, new Text(nickname));
            }
        }
    }

    // Mapper for Associates to emit relationships
    public static class AssociatesMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                String id1 = fields[1]; // id1
                String id2 = fields[2]; // id2
                context.write(new Text(id1), new Text("happiness#1"));
                context.write(new Text(id2), new Text("happiness#1"));
            }
        }
    }


    public static class HappinessReducer extends Reducer<Text, Text, Text, IntWritable> {

        private Map<String, Integer> happinessMap = new HashMap<>();
        private Map<String, String> nicknameMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nickname = "";
            int happinessFactor = 0;


            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("nickname#")) {
                    nickname = value.substring(9);  // Remove "nickname#" prefix
                    nicknameMap.put(key.toString(), nickname);
                } else if (value.startsWith("happiness#")) {

                    happinessFactor += 1;
                }
            }

            if (!nickname.isEmpty()) {
                happinessMap.put(key.toString(), happinessFactor);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output all nicknames with their happiness factors
            for (String id : happinessMap.keySet()) {
                context.write(new Text(nicknameMap.get(id)), new IntWritable(happinessMap.get(id)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: HappinessFactorJob <linkbook path> <associates path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Happiness Factor Job");
        job.setJarByClass(HappyCount.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, LinkBookPageMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AssociatesMapper.class);
        job.setCombinerClass(HappinessReducer.class);

        job.setReducerClass(HappinessReducer.class);

        // Specify output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Output path
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}