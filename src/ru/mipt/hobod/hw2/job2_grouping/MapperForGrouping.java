package ru.mipt.hobod.hw2.job2_grouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by dmitry on 30.03.17.
 */
public class MapperForGrouping extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\t");
        context.write(new Text(values[0]), new Text(values[1] + "\t" + values[2]));
    }
}
