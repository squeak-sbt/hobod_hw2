package ru.mipt.hobod.hw2.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by dmitry on 26.03.17.
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        String id = null;
        String postTypeId = null;
        String ownerId = null;
        String reputation = null;
        String score = null;
        String parentId = null;

        for (String field : fields) {
            String[] attribute = field.split("=");
            if (attribute.length < 2) {
                continue;
            }
            if (attribute[0].equals("Id")) {
                id = attribute[1].replace("\"", "");
            }
            if (attribute[0].equals("PostTypeId")) {
                postTypeId = attribute[1].replace("\"", "");
            }
            if (attribute[0].equals("OwnerId")) {
                ownerId = attribute[1].replace("\"", "");
            }
            if (attribute[0].equals("Score")) {
                score = attribute[1].replace("\"", "");
            }
            if (attribute[0].equals("Reputation")) {
                reputation = attribute[1].replace("\"", "");
            }
            if (attribute[0].equals("ParentId")) {
                parentId = attribute[1].replace("\"", "");
            }
        }
        if (reputation != null) { //It is USER
            context.write(new Text(id), new Text("U," + reputation));
        }
        else if (postTypeId != null) {
            /*if (postTypeId.equals("1")) { // IT is QUESTION
                if (ownerId != null) {
                    context.write(new Text(ownerId), new Text("Q," + id));
                }
            }
            else if (postTypeId.equals("2")) {
                if (ownerId != null && score != null && parentId != null) {
                    context.write(new Text(ownerId), new Text("A," + parentId + "," + score));
                }
            }*/
            if (postTypeId.equals("2")) {
                if (ownerId != null && score != null && parentId != null) {
                    context.write(new Text(ownerId), new Text("A," + parentId + "," + score));
                }
            }
        }
    }


}
