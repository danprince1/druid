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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.incremental.MutableRowIngestionMeters;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ParallelIndexStatsReporter
{
  private static final Logger LOG = new Logger(ParallelIndexStatsReporter.class);

  abstract ParallelIndexStats report(
      ParallelIndexSupervisorTask task,
      Object runner,
      boolean includeUnparseable,
      String full
  );

  protected RowIngestionMetersTotals getBuildSegmentsStatsFromTaskReport(
      Map<String, TaskReport> taskReport,
      boolean includeUnparseable,
      List<ParseExceptionReport> unparseableEvents
  )
  {
    IngestionStatsAndErrorsTaskReport ingestionStatsAndErrorsReport =
        (IngestionStatsAndErrorsTaskReport) taskReport.get(
            IngestionStatsAndErrorsTaskReport.REPORT_KEY);
    IngestionStatsAndErrorsTaskReportData reportData =
        (IngestionStatsAndErrorsTaskReportData) ingestionStatsAndErrorsReport.getPayload();
    RowIngestionMetersTotals totals = getTotalsFromBuildSegmentsRowStats(
        reportData.getRowStats().get(RowIngestionMeters.BUILD_SEGMENTS)
    );
    if (includeUnparseable) {
      List<ParseExceptionReport> taskUnparsebleEvents =
          (List<ParseExceptionReport>) reportData.getUnparseableEvents().get(RowIngestionMeters.BUILD_SEGMENTS);
      unparseableEvents.addAll(taskUnparsebleEvents);
    }
    return totals;
  }

  private RowIngestionMetersTotals getTotalsFromBuildSegmentsRowStats(Object buildSegmentsRowStats)
  {
    if (buildSegmentsRowStats instanceof RowIngestionMetersTotals) {
      // This case is for unit tests. Normally when deserialized the row stats will apppear as a Map<String, Object>.
      return (RowIngestionMetersTotals) buildSegmentsRowStats;
    } else if (buildSegmentsRowStats instanceof Map) {
      Map<String, Object> buildSegmentsRowStatsMap = (Map<String, Object>) buildSegmentsRowStats;
      return new RowIngestionMetersTotals(
          ((Number) buildSegmentsRowStatsMap.get("processed")).longValue(),
          ((Number) buildSegmentsRowStatsMap.get("processedWithError")).longValue(),
          ((Number) buildSegmentsRowStatsMap.get("thrownAway")).longValue(),
          ((Number) buildSegmentsRowStatsMap.get("unparseable")).longValue()
      );
    } else {
      // should never happen
      throw new RuntimeException("Unrecognized buildSegmentsRowStats type: " + buildSegmentsRowStats.getClass());
    }
  }

  protected RowIngestionMetersTotals getRowStatsAndUnparseableEventsForRunningTasks(
      ParallelIndexSupervisorTask task,
      Set<String> runningTaskIds,
      List<ParseExceptionReport> unparseableEvents,
      boolean includeUnparseable
  )
  {
    final MutableRowIngestionMeters buildSegmentsRowStats = new MutableRowIngestionMeters();
    for (String runningTaskId : runningTaskIds) {
      try {
        final Map<String, Object> report = task.fetchTaskReport(runningTaskId);
        if (report == null || report.isEmpty()) {
          // task does not have a running report yet
          continue;
        }

        Map<String, Object> ingestionStatsAndErrors = (Map<String, Object>) report.get("ingestionStatsAndErrors");
        Map<String, Object> payload = (Map<String, Object>) ingestionStatsAndErrors.get("payload");
        Map<String, Object> rowStats = (Map<String, Object>) payload.get("rowStats");
        Map<String, Object> totals = (Map<String, Object>) rowStats.get("totals");
        Map<String, Object> buildSegments = (Map<String, Object>) totals.get(RowIngestionMeters.BUILD_SEGMENTS);

        if (includeUnparseable) {
          Map<String, Object> taskUnparseableEvents = (Map<String, Object>) payload.get("unparseableEvents");
          List<ParseExceptionReport> buildSegmentsUnparseableEvents = (List<ParseExceptionReport>)
              taskUnparseableEvents.get(RowIngestionMeters.BUILD_SEGMENTS);
          unparseableEvents.addAll(buildSegmentsUnparseableEvents);
        }

        buildSegmentsRowStats.addRowIngestionMetersTotals(getTotalsFromBuildSegmentsRowStats(buildSegments));
      }
      catch (Exception e) {
        LOG.warn(e, "Encountered exception when getting live subtask report for task: " + runningTaskId);
      }
    }
    return buildSegmentsRowStats.getTotals();
  }

  protected Pair<Map<String, Object>, Map<String, Object>> createStatsAndErrorsReport(
      RowIngestionMetersTotals rowStats,
      List<ParseExceptionReport> unparseableEvents
  )
  {
    Map<String, Object> rowStatsMap = new HashMap<>();
    Map<String, Object> totalsMap = new HashMap<>();
    totalsMap.put(RowIngestionMeters.BUILD_SEGMENTS, rowStats);
    rowStatsMap.put("totals", totalsMap);

    return Pair.of(rowStatsMap, ImmutableMap.of(RowIngestionMeters.BUILD_SEGMENTS, unparseableEvents));
  }

}
