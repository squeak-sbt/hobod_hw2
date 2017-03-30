package ru.mipt.hobod.hw2.job2_grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by dmitry on 30.03.17.
 */
public class ReducerForGrouping extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<ScoreReputation> maxScoreSet = new TreeSet<>(new Comparator<ScoreReputation>() {
            @Override
            public int compare(ScoreReputation o1, ScoreReputation o2) {
                return -Integer.compare(o1.getScore(), o2.getScore());
            }
        });
        Set<ScoreReputation> maxReputationSet = new TreeSet<>(new Comparator<ScoreReputation>() {
            @Override
            public int compare(ScoreReputation o1, ScoreReputation o2) {
                return -Integer.compare(o1.getReputation(), o2.getReputation());
            }
        });

        for (Text value : values) {
            String[] scores = value.toString().split("\\t");
            try {
                ScoreReputation scoreReputation = new ScoreReputation(Integer.valueOf(scores[0]), Integer.valueOf(scores[1]));
                maxScoreSet.add(scoreReputation);
                maxReputationSet.add(scoreReputation);
            }
            catch (NumberFormatException e) {

            }
        }
        Iterator<ScoreReputation> iteratorScore = maxScoreSet.iterator();
        Iterator<ScoreReputation> iteratorReputation = maxReputationSet.iterator();
        if (iteratorScore.hasNext() && iteratorReputation.hasNext()) {
            if (!iteratorScore.next().equals(iteratorReputation.next())) {
                context.getCounter(MyCounter.MISMATCH_POSTS).increment(1);
            }
        }

        context.write(key, NullWritable.get());
    }
}
