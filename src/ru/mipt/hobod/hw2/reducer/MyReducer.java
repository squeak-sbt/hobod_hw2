package ru.mipt.hobod.hw2.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by dmitry on 26.03.17.
 */
public class MyReducer extends Reducer<Text, Text, Text, NullWritable> {
    private static volatile Set<String> postSet = new HashSet<>();

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
        for (Text value : values) {
            context.write(value, NullWritable.get());
        }
    }
}
