package ru.mipt.hobod.hw2.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by dmitry on 26.03.17.
 */
public class MyReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /*int reputation = 0;
        int maxScore = 0;
        for (Text value : values) {
            String[] params = value.toString().split(",");
            if (params[0].equals("3")) {
                reputation = Integer.valueOf(params[1]);
            }
            if (params[0].equals("1")) {

            }
        }*/
        /*for (Text value : values) {
            context.write(value, NullWritable.get());
        }*/
        String reputation = null;
        boolean found = false;
        for (Text value : values) {
            String[] split = value.toString().split(",");
            if (split[0].equals("U")) {
                reputation = split[1];
                found = true;
                break;
            }
        }
        if (found) {
            for (Text value : values) {
                String[] attributes = value.toString().split(",");
                if (attributes[0].equals("A")) {
                    context.write(new Text(attributes[1]), new Text(attributes[2] + "\t" + reputation));
                }
            }
        }
    }
}
