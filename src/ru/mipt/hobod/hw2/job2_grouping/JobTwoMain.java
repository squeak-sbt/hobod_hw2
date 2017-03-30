package ru.mipt.hobod.hw2.job2_grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by dmitry on 30.03.17.
 */
public class JobTwoMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new JobTwoMain(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        @SuppressWarnings("deprecation")
        Job job = new Job(conf);

        job.setJarByClass(ru.mipt.hobod.hw2.job2_grouping.JobTwoMain.class);

        job.setMapperClass(MapperForGrouping.class);
        job.setReducerClass(ReducerForGrouping.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(4);


        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        job.waitForCompletion(true);

        
        long value = job.getCounters().findCounter(MyCounter.MISMATCH_POSTS).getValue();
        System.out.println(value);

        return 0;
    }
}
