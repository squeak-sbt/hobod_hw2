package ru.mipt.hobod.hw2.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by dmitry on 28.03.17.
 */
public class PostsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] attributes = value.toString().split(" ");
        String type = null, ownerId = null, parentId = null, score = null;
        for (String attribute : attributes) {
            String[] fields = attribute.split("=");
            if (fields.length < 2) {
                continue;
            }
            if (fields[0].equals("PostTypeId")) {
                type = fields[1].replace("\"", "");
            }
            if (fields[0].equals("OwnerId")) {
                ownerId = fields[1];
            }
            if (fields[0].equals("ParentId")) {
                parentId = fields[1];
            }
            if (fields[0].equals("Score")) {
                score = fields[1];
            }
        }
        if (type != null && ownerId != null && parentId != null && score != null) {
            if (type.equals("2"))
                context.write(new Text(ownerId), new Text("A," + parentId + "," + score));
        }
    }
}
