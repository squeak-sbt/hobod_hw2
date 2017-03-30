package ru.mipt.hobod.hw2.job1_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by dmitry on 26.03.17.
 */
public class JobOneMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new JobOneMain(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        @SuppressWarnings("deprecation")
            Job job = new Job(conf);

        job.setJarByClass(JobOneMain.class);

        job.setMapperClass(MapperForJoin.class);
        job.setReducerClass(ReducerForJoin.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(4);


        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        job.waitForCompletion(true);

        return 0;
    }
}
