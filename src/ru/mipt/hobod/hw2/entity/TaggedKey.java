package ru.mipt.hobod.hw2.entity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by dmitry on 26.03.17.
 */
public class TaggedKey implements WritableComparable<TaggedKey> {
    private Text value;
    private IntWritable tag;




    @Override
    public int compareTo(TaggedKey o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        value.write(dataOutput);
        tag.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value.readFields(dataInput);
        tag.readFields(dataInput);
    }
}
