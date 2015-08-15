/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.metrics.stats;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;


/**
 * The rate of the given quantity. By default this is the total observed over a set of samples from a sampled statistic
 * divided by the elapsed time over the sample windows. Alternative {@link SampledStat} implementations can be provided,
 * however, to record the rate of occurrences (e.g. the count of values measured over the time interval) or other such
 * values.
 */
public class Rate implements MeasurableStat {

    private final TimeUnit unit;
    private final SampledStat stat;

    public Rate() {
        this(TimeUnit.SECONDS);
    }

    public Rate(TimeUnit unit) {
        this(unit, new SampledTotal());
    }

    public Rate(SampledStat stat) {
        this(TimeUnit.SECONDS, stat);
    }

    public Rate(TimeUnit unit, SampledStat stat) {
        this.stat = stat;
        this.unit = unit;
    }

    public String unitName() {
        return unit.name().substring(0, unit.name().length() - 2).toLowerCase();
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        this.stat.record(config, value, timeMs);
    }

    @Override
    public double measure(MetricConfig config, long now) {
        double value = stat.measure(config, now);
        // the elapsed time is always N-1 complete windows plus whatever fraction of the final window is complete
        long elapsedCurrentWindowMs = now - stat.current(now).lastWindowMs;
        long elapsedPriorWindowsMs = config.timeWindowMs() * (config.samples() - 1);
        return value / convert(elapsedCurrentWindowMs + elapsedPriorWindowsMs);
    }

    private double convert(long timeMs) {
        switch (unit) {
            case NANOSECONDS:
                return timeMs * 1000.0 * 1000.0;
            case MICROSECONDS:
                return timeMs * 1000.0;
            case MILLISECONDS:
                return timeMs;
            case SECONDS:
                return timeMs / 1000.0;
            case MINUTES:
                return timeMs / (60.0 * 1000.0);
            case HOURS:
                return timeMs / (60.0 * 60.0 * 1000.0);
            case DAYS:
                return timeMs / (24.0 * 60.0 * 60.0 * 1000.0);
            default:
                throw new IllegalStateException("Unknown unit: " + unit);
        }
    }

    public static class SampledTotal extends SampledStat {

        public SampledTotal() {
            super(0.0d);
        }

        @Override
        protected void update(Sample sample, MetricConfig config, double value, long timeMs) {
            sample.value += value;
        }

        @Override
        public double combine(List<Sample> samples, MetricConfig config, long now) {
            double total = 0.0;
            for (int i = 0; i < samples.size(); i++)
                total += samples.get(i).value;
            return total;
        }

    }
}
