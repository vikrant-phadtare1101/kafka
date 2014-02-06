package kafka.common.metrics.stats;

import java.util.List;

import kafka.common.metrics.MetricConfig;

/**
 * A {@link SampledStat} that gives the max over its samples.
 */
public final class Max extends SampledStat {

    public Max() {
        super(Double.NEGATIVE_INFINITY);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value = Math.max(sample.value, value);
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < samples.size(); i++)
            max = Math.max(max, samples.get(i).value);
        return max;
    }

}
