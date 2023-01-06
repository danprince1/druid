/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexer;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 */
public class HadoopDruidDetermineConfigurationJob implements Jobby
{
  private static final Logger log = new Logger(HadoopDruidDetermineConfigurationJob.class);
  private final HadoopDruidIndexerConfig config;
  private Jobby job;
  private String hadoopJobIdFile;
  private String failureCause = "";

  @Inject
  public HadoopDruidDetermineConfigurationJob(HadoopDruidIndexerConfig config)
  {
    this.config = config;
  }

  @Override
  public boolean run()
  {
    JobHelper.ensurePaths(config);

    if (config.isDeterminingPartitions()) {
      job = createPartitionJob(config);
      config.setHadoopJobIdFileName(hadoopJobIdFile);
      boolean jobSucceeded = JobHelper.runSingleJob(job);

      if (jobSucceeded) {
        if (config.getSchema().getTuningConfig().getMaxIntervalsIngested() < Integer.MAX_VALUE) {
          // Check if this ingestion job breached the maxIntervalsIngested value, requiring early exit
          jobSucceeded = JobHelper.evaluateMaxIntervalsIngestedCircuitBreaker(config.getSchema());
          if (!jobSucceeded) {
            this.failureCause = "maxIntervalsIngested threshold breached";
          }
        }
        if (jobSucceeded && config.getSchema().getTuningConfig().getMaxSegmentsIngested() < Integer.MAX_VALUE) {
          // Check if this ingestion job breached the maxSegmentsIngested value, requiring early exit.
          // We check jobSucceeded first because breaching intervals threshold trumps breaching segments threshold
          jobSucceeded = JobHelper.evaluateMaxSegmentsIngestedCircuitBreaker(config.getSchema());
          if (!jobSucceeded) {
            this.failureCause = "maxSegmentsIngested threshold breached";
          }
        }
      }

      JobHelper.maybeDeleteIntermediatePath(
          jobSucceeded,
          config.getSchema()
      );
      return jobSucceeded;
    } else {
      final PartitionsSpec partitionsSpec = config.getPartitionsSpec();
      final int shardsPerInterval;
      final HashPartitionFunction partitionFunction;
      if (partitionsSpec instanceof HashedPartitionsSpec) {
        final HashedPartitionsSpec hashedPartitionsSpec = (HashedPartitionsSpec) partitionsSpec;
        shardsPerInterval = PartitionsSpec.isEffectivelyNull(hashedPartitionsSpec.getNumShards())
                            ? 1
                            : hashedPartitionsSpec.getNumShards();
        partitionFunction = hashedPartitionsSpec.getPartitionFunction();
      } else {
        shardsPerInterval = 1;
        partitionFunction = null;
      }
      Map<Long, List<HadoopyShardSpec>> shardSpecs = new TreeMap<>();
      int shardCount = 0;
      for (Interval segmentGranularity : config.getSegmentGranularIntervals()) {
        DateTime bucket = segmentGranularity.getStart();
        // negative shardsPerInterval means a single shard
        List<HadoopyShardSpec> specs = Lists.newArrayListWithCapacity(shardsPerInterval);
        for (int i = 0; i < shardsPerInterval; i++) {
          specs.add(
              new HadoopyShardSpec(
                  new HashBasedNumberedShardSpec(
                      i,
                      shardsPerInterval,
                      i,
                      shardsPerInterval,
                      config.getPartitionsSpec().getPartitionDimensions(),
                      partitionFunction,
                      HadoopDruidIndexerConfig.JSON_MAPPER
                  ),
                  shardCount++
              )
          );
        }
        shardSpecs.put(bucket.getMillis(), specs);
        log.info("DateTime[%s], spec[%s]", bucket, specs);
      }
      config.setShardSpecs(shardSpecs);
      return true;
    }
  }

  private static Jobby createPartitionJob(HadoopDruidIndexerConfig config)
  {
    final PartitionsSpec partitionsSpec = config.getPartitionsSpec();
    if (partitionsSpec instanceof HashedPartitionsSpec) {
      return new DetermineHashedPartitionsJob(config);
    } else if (partitionsSpec instanceof SingleDimensionPartitionsSpec) {
      return new DeterminePartitionsJob(config);
    } else {
      throw new ISE("Unknown partitionsSpec[%s]", partitionsSpec);
    }
  }

  @Override
  public Map<String, Object> getStats()
  {
    if (job == null) {
      return null;
    }

    return job.getStats();
  }

  @Override
  public String getErrorMessage()
  {
    if (job == null) {
      return null;
    } else if (!failureCause.isEmpty()) {
      return failureCause;
    } else {
      return job.getErrorMessage();
    }
  }

  public void setHadoopJobIdFile(String hadoopJobIdFile)
  {
    this.hadoopJobIdFile = hadoopJobIdFile;
  }
}
