package ru.mipt.hobod.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.mipt.hobod.hw2.mapper.MyMapper;
import ru.mipt.hobod.hw2.reducer.MyReducer;

/**
 * Created by dmitry on 26.03.17.
 */
public class MainClass extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MainClass(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        @SuppressWarnings("deprecation")
            Job job = new Job(conf);

        job.setJarByClass(MainClass.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(8);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        job.waitForCompletion(true);

        return 0;
    }
}
