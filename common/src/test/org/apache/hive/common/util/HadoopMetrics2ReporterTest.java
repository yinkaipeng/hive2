/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.common.util;

import org.apache.hive.common.util.HadoopMetrics2Reporter;
import org.apache.hive.common.util.HadoopMetrics2Reporter.Builder;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link HadoopMetrics2Reporter}.
 *
 * Please do not modify, this class is here only temporarily, and is being introduced
 * in a Deprecated state for
 */
@Deprecated
public class HadoopMetrics2ReporterTest {

  private MetricRegistry mockRegistry;
  private MetricsSystem mockMetricsSystem;
  private String recordName = "myserver";
  private HadoopMetrics2Reporter metrics2Reporter;

  @Before public void setup() {
    mockRegistry = Mockito.mock(MetricRegistry.class);
    mockMetricsSystem = Mockito.mock(MetricsSystem.class);

    recordName = "myserver";
    metrics2Reporter = HadoopMetrics2Reporter.forRegistry(mockRegistry)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build(mockMetricsSystem, "MyServer", "My Cool Server", recordName);
  }

  private void verifyRecordBuilderUnits(MetricsRecordBuilder recordBuilder) {
    Mockito.verify(recordBuilder).tag(HadoopMetrics2Reporter.RATE_UNIT_LABEL,
        metrics2Reporter.getRateUnit());
    Mockito.verify(recordBuilder).tag(HadoopMetrics2Reporter.DURATION_UNIT_LABEL,
        metrics2Reporter.getDurationUnit());
  }

  @Test public void testBuilderDefaults() {
    Builder builder = HadoopMetrics2Reporter.forRegistry(mockRegistry);

    final String jmxContext = "MyJmxContext;sub=Foo";
    final String desc = "Description";
    final String recordName = "Metrics";

    HadoopMetrics2Reporter reporter =
        builder.build(mockMetricsSystem, jmxContext, desc, recordName);

    assertEquals(mockMetricsSystem, reporter.getMetrics2System());
    // The Context "tag", not the jmx context
    assertEquals(null, reporter.getContext());
    assertEquals(recordName, reporter.getRecordName());
  }

  @Test public void testGaugeReporting() {
    final AtomicLong gaugeValue = new AtomicLong(0L);
    @SuppressWarnings("rawtypes")
    final Gauge gauge = new Gauge<Long>() {
      @Override public Long getValue() {
        return gaugeValue.get();
      }
    };

    // Add the metrics objects to the internal "queues" by hand
    metrics2Reporter.getDropwizardGauges().add(new SimpleEntry<>("my_gauge", gauge));

    // Set some values
    gaugeValue.set(5L);

    MetricsCollector collector = Mockito.mock(MetricsCollector.class);
    MetricsRecordBuilder recordBuilder = Mockito.mock(MetricsRecordBuilder.class);

    Mockito.when(collector.addRecord(recordName)).thenReturn(recordBuilder);

    // Make sure a value of 5 gets reported
    metrics2Reporter.getMetrics(collector, true);

    Mockito.verify(recordBuilder).addGauge(Interns.info("my_gauge", ""), gaugeValue.get());
    verifyRecordBuilderUnits(recordBuilder);
  }

  @Test public void testCounterReporting() {
    final Counter counter = new Counter();

    // Add the metrics objects to the internal "queues" by hand
    metrics2Reporter.getDropwizardCounters().add(new SimpleEntry<>("my_counter", counter));

    // Set some values
    counter.inc(5L);

    MetricsCollector collector = Mockito.mock(MetricsCollector.class);
    MetricsRecordBuilder recordBuilder = Mockito.mock(MetricsRecordBuilder.class);

    Mockito.when(collector.addRecord(recordName)).thenReturn(recordBuilder);

    metrics2Reporter.getMetrics(collector, true);

    Mockito.verify(recordBuilder).addCounter(Interns.info("my_counter", ""), 5L);
    verifyRecordBuilderUnits(recordBuilder);
  }

  @Test public void testHistogramReporting() {
    final String metricName = "my_histogram";
    final Histogram histogram = Mockito.mock(Histogram.class);
    final Snapshot snapshot = Mockito.mock(Snapshot.class);

    long count = 10L;
    double percentile75 = 75;
    double percentile95 = 95;
    double percentile98 = 98;
    double percentile99 = 99;
    double percentile999 = 999;
    double median = 50;
    double mean = 60;
    long min = 1L;
    long max = 100L;
    double stddev = 10;

    Mockito.when(snapshot.get75thPercentile()).thenReturn(percentile75);
    Mockito.when(snapshot.get95thPercentile()).thenReturn(percentile95);
    Mockito.when(snapshot.get98thPercentile()).thenReturn(percentile98);
    Mockito.when(snapshot.get99thPercentile()).thenReturn(percentile99);
    Mockito.when(snapshot.get999thPercentile()).thenReturn(percentile999);
    Mockito.when(snapshot.getMedian()).thenReturn(median);
    Mockito.when(snapshot.getMean()).thenReturn(mean);
    Mockito.when(snapshot.getMin()).thenReturn(min);
    Mockito.when(snapshot.getMax()).thenReturn(max);
    Mockito.when(snapshot.getStdDev()).thenReturn(stddev);

    Mockito.when(histogram.getCount()).thenReturn(count);
    Mockito.when(histogram.getSnapshot()).thenReturn(snapshot);

    MetricsCollector collector = Mockito.mock(MetricsCollector.class);
    MetricsRecordBuilder recordBuilder = Mockito.mock(MetricsRecordBuilder.class);

    Mockito.when(collector.addRecord(recordName)).thenReturn(recordBuilder);

    // Add the metrics objects to the internal "queues" by hand
    metrics2Reporter.getDropwizardHistograms().add(new SimpleEntry<>(metricName, histogram));

    metrics2Reporter.getMetrics(collector, true);

    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_max", ""),
        metrics2Reporter.convertDuration(max));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_min", ""),
        metrics2Reporter.convertDuration(min));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_median", ""),
        metrics2Reporter.convertDuration(median));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_count", ""), count);
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_stddev", ""),
        metrics2Reporter.convertDuration(stddev));

    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_75thpercentile", ""),
        metrics2Reporter.convertDuration(percentile75));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_95thpercentile", ""),
        metrics2Reporter.convertDuration(percentile95));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_98thpercentile", ""),
        metrics2Reporter.convertDuration(percentile98));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_99thpercentile", ""),
        metrics2Reporter.convertDuration(percentile99));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_999thpercentile", ""),
        metrics2Reporter.convertDuration(percentile999));

    verifyRecordBuilderUnits(recordBuilder);
  }

  @Test public void testTimerReporting() {
    final String metricName = "my_timer";
    final Timer timer = Mockito.mock(Timer.class);
    final Snapshot snapshot = Mockito.mock(Snapshot.class);

    // Add the metrics objects to the internal "queues" by hand
    metrics2Reporter.getDropwizardTimers().add(new SimpleEntry<>(metricName, timer));

    long count = 10L;
    double meanRate = 1.0;
    double oneMinRate = 2.0;
    double fiveMinRate = 5.0;
    double fifteenMinRate = 10.0;

    Mockito.when(timer.getCount()).thenReturn(count);
    Mockito.when(timer.getMeanRate()).thenReturn(meanRate);
    Mockito.when(timer.getOneMinuteRate()).thenReturn(oneMinRate);
    Mockito.when(timer.getFiveMinuteRate()).thenReturn(fiveMinRate);
    Mockito.when(timer.getFifteenMinuteRate()).thenReturn(fifteenMinRate);
    Mockito.when(timer.getSnapshot()).thenReturn(snapshot);

    double percentile75 = 75;
    double percentile95 = 95;
    double percentile98 = 98;
    double percentile99 = 99;
    double percentile999 = 999;
    double median = 50;
    double mean = 60;
    long min = 1L;
    long max = 100L;
    double stddev = 10;

    Mockito.when(snapshot.get75thPercentile()).thenReturn(percentile75);
    Mockito.when(snapshot.get95thPercentile()).thenReturn(percentile95);
    Mockito.when(snapshot.get98thPercentile()).thenReturn(percentile98);
    Mockito.when(snapshot.get99thPercentile()).thenReturn(percentile99);
    Mockito.when(snapshot.get999thPercentile()).thenReturn(percentile999);
    Mockito.when(snapshot.getMedian()).thenReturn(median);
    Mockito.when(snapshot.getMean()).thenReturn(mean);
    Mockito.when(snapshot.getMin()).thenReturn(min);
    Mockito.when(snapshot.getMax()).thenReturn(max);
    Mockito.when(snapshot.getStdDev()).thenReturn(stddev);

    MetricsCollector collector = Mockito.mock(MetricsCollector.class);
    MetricsRecordBuilder recordBuilder = Mockito.mock(MetricsRecordBuilder.class);

    Mockito.when(collector.addRecord(recordName)).thenReturn(recordBuilder);

    metrics2Reporter.getMetrics(collector, true);

    // We get the count from the meter and histogram
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_count", ""), count);

    // Verify the rates
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_mean_rate", ""),
        metrics2Reporter.convertRate(meanRate));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_1min_rate", ""),
        metrics2Reporter.convertRate(oneMinRate));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_5min_rate", ""),
        metrics2Reporter.convertRate(fiveMinRate));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_15min_rate", ""),
        metrics2Reporter.convertRate(fifteenMinRate));

    // Verify the histogram
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_max", ""),
        metrics2Reporter.convertDuration(max));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_min", ""),
        metrics2Reporter.convertDuration(min));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_median", ""),
        metrics2Reporter.convertDuration(median));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_stddev", ""),
        metrics2Reporter.convertDuration(stddev));

    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_75thpercentile", ""),
        metrics2Reporter.convertDuration(percentile75));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_95thpercentile", ""),
        metrics2Reporter.convertDuration(percentile95));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_98thpercentile", ""),
        metrics2Reporter.convertDuration(percentile98));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_99thpercentile", ""),
        metrics2Reporter.convertDuration(percentile99));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_999thpercentile", ""),
        metrics2Reporter.convertDuration(percentile999));

    verifyRecordBuilderUnits(recordBuilder);
  }

  @Test public void testMeterReporting() {
    final String metricName = "my_meter";
    final Meter meter = Mockito.mock(Meter.class);

    // Add the metrics objects to the internal "queues" by hand
    metrics2Reporter.getDropwizardMeters().add(new SimpleEntry<>(metricName, meter));

    // Set some values
    long count = 10L;
    double meanRate = 1.0;
    double oneMinRate = 2.0;
    double fiveMinRate = 5.0;
    double fifteenMinRate = 10.0;

    Mockito.when(meter.getCount()).thenReturn(count);
    Mockito.when(meter.getMeanRate()).thenReturn(meanRate);
    Mockito.when(meter.getOneMinuteRate()).thenReturn(oneMinRate);
    Mockito.when(meter.getFiveMinuteRate()).thenReturn(fiveMinRate);
    Mockito.when(meter.getFifteenMinuteRate()).thenReturn(fifteenMinRate);

    MetricsCollector collector = Mockito.mock(MetricsCollector.class);
    MetricsRecordBuilder recordBuilder = Mockito.mock(MetricsRecordBuilder.class);

    Mockito.when(collector.addRecord(recordName)).thenReturn(recordBuilder);

    metrics2Reporter.getMetrics(collector, true);

    // Verify the rates
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_count", ""), count);
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_mean_rate", ""),
        metrics2Reporter.convertRate(meanRate));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_1min_rate", ""),
        metrics2Reporter.convertRate(oneMinRate));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_5min_rate", ""),
        metrics2Reporter.convertRate(fiveMinRate));
    Mockito.verify(recordBuilder).addGauge(Interns.info(metricName + "_15min_rate", ""),
        metrics2Reporter.convertRate(fifteenMinRate));

    // Verify the units
    verifyRecordBuilderUnits(recordBuilder);
  }
}
