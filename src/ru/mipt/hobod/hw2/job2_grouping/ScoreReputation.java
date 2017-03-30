package ru.mipt.hobod.hw2.job2_grouping;

/**
 * Created by dmitry on 30.03.17.
 */
public class ScoreReputation {
    private final int score;
    private final int reputation;

    public ScoreReputation(int score, int reputation) {
        this.score = score;
        this.reputation = reputation;
    }

    public int getScore() {
        return score;
    }

    public int getReputation() {
        return reputation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScoreReputation that = (ScoreReputation) o;

        if (score != that.score) return false;
        return reputation == that.reputation;

    }

    @Override
    public int hashCode() {
        int result = score;
        result = 31 * result + 43 * reputation;
        return result;
    }
}
