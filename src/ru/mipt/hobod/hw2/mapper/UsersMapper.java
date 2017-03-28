package ru.mipt.hobod.hw2.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by dmitry on 28.03.17.
 */
public class UsersMapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] attributes = value.toString().split(" ");
        String id = null, reputation = null;
        for (String attribute : attributes) {
            String[] fields = attribute.split("=");
            if (fields.length < 2) {
                continue;
            }
            if (fields[0].equals("Reputation")) {
                reputation = fields[1].replace("\"", "");
            }
            if (fields[0].equals("Id")) {
                id = fields[1].replace("\"", "");
            }
        }
        if (id != null && reputation != null) {
            context.write(new Text(id), new Text("U," + reputation));
        }
    }
}
