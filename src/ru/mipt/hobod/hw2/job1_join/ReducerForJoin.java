package ru.mipt.hobod.hw2.job1_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dmitry on 26.03.17.
 */
public class ReducerForJoin extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String[]> answers = new ArrayList<>();
        String reputation = null;
        boolean found = false;
        for (Text value : values) {
            String[] split = value.toString().split(",");
            if (!found) {
                if (split[0].equals("U")) {
                    reputation = split[1];
                    found = true;
                }
                else if (split[0].equals("A") && split.length == 3) {
                    answers.add(split);
                }
            }
            else if (split[0].equals("A") && split.length == 3) {
                answers.add(split);
            }
        }
        if (found) {
            for (String[] attributes : answers) {
                context.write(new Text(attributes[1]), new Text(attributes[2] + "\t" + reputation));
            }
        }
    }
}
