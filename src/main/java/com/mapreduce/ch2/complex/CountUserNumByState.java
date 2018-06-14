package com.mapreduce.ch2.complex;

import com.mapreduce.utils.MRDPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Learn how to count with counters(only need mapper)
 *
 * @author zhanglei
 * 2018.06.14
 */
public class CountUserNumByState {
    public static class CountUserNumByStateMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
        public static final String STATE_COUNTER_GROUP = "State";

        private String[] statesArray = new String[]{};

        private Set<String> states = new HashSet<String>(Arrays.asList(statesArray));

        public void map(Object key, Text value, Context context) {
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

            String location = parsed.get("Location");

            if(location != null && location.length() > 0) {
                boolean unknown = true;

                String[] tokens = location.toUpperCase().split("\\s"); // \s means space character
                for(String state : tokens) {
                    if(states.contains(state)) {
                        context.getCounter(STATE_COUNTER_GROUP, state).increment(1);
                        unknown = false;
                        break;
                    }
                }

                // If the state is unknown
                if(unknown) {
                    context.getCounter(STATE_COUNTER_GROUP, "Unknown").increment(1);
                }
            } else {
                context.getCounter(STATE_COUNTER_GROUP, "NullOrEmpty").increment(1);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // GenericOptionsParser是hadoop框架中解析命令行参数的基本类，能识别一些标准的命令行参数
        // getRemainingArgs则用于获取剩余的参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length != 2) {
            System.out.println("Usage: CountUserNumByState <users> <out>");
            System.exit(2);
        }

        Path input = new Path(otherArgs[0]);
        Path outputDir = new Path(otherArgs[1]);

        Job job = new Job(conf, "Count user num by state");
        job.setJarByClass(CountUserNumByState.class);

        job.setMapperClass(CountUserNumByStateMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        int code = job.waitForCompletion(true) ? 0 : 1;
        if(code == 0) {
            for(Counter counter : job.getCounters().getGroup(CountUserNumByStateMapper.STATE_COUNTER_GROUP)) {
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
            }
        }

        // Clean up empty output directory
        FileSystem.get(conf).delete(outputDir, true);
        System.exit(code);
    }
}
