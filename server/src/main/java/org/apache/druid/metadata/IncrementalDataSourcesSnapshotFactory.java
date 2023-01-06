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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DataSourcesSnapshotFactory;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.exceptions.UnableToCreateStatementException;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Builds a new {@link DataSourcesSnapshot} by making incremental changes to the previous snapshot.
 * NOTE:  the problem with not querying all segments on every poll in SqlSegmentsMetadataManager
 * is that we can miss a segment that is used on one poll and deleted from the metastore
 * on the next poll.  Such a segment will be present and 'used' in every subsequent snapshot, even though it should be
 * missing.  To prevent this situation, the segment kill policy (if enabled) must be such that an unused
 * segment exists in an unused state for at least the amount of time between two polls.
 * As a result, we blow up at configuration time if such a kill policy is not configured.
 */
public class IncrementalDataSourcesSnapshotFactory implements DataSourcesSnapshotFactory
{
  private static final EmittingLogger log = new EmittingLogger(IncrementalDataSourcesSnapshotFactory.class);

  private static ImmutableMap<String, String> createDefaultDataSourceProperties()
  {
    return ImmutableMap.of("created", DateTimes.nowUtc().toString());
  }

  /**
   * Immutable wrapper for DataSegment that adds a <code>used</code> flag.
   */
  @VisibleForTesting
  static class Segment
  {
    final DataSegment segment;
    final boolean used;

    Segment(final DataSegment dataSegment, final boolean used)
    {
      this.segment = dataSegment;
      this.used = used;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Segment segment1 = (Segment) o;
      return used == segment1.used && Objects.equals(segment, segment1.segment);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(segment, used);
    }
  }

  static class QueryResult
  {
    final ImmutableSet<DataSegment> unusedSegments;
    final ImmutableSet<DataSegment> usedSegments;

    QueryResult(final Collection<DataSegment> usedSegments, final Collection<DataSegment> unusedSegments)
    {
      this.usedSegments = ImmutableSet.copyOf(usedSegments);
      this.unusedSegments = ImmutableSet.copyOf(unusedSegments);
    }
  }


  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final ObjectMapper jsonMapper;
  private final SQLMetadataConnector connector;

  @Inject
  public IncrementalDataSourcesSnapshotFactory(
      Supplier<MetadataStorageTablesConfig> dbTables,
      ObjectMapper jsonMapper,
      SQLMetadataConnector connector,
      SegmentsMetadataManagerConfig segmentsMetadataManagerConfig,
      DruidCoordinatorConfig druidCoordinatorConfig
  )
  {
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.connector = connector;
    validateConfiguration(segmentsMetadataManagerConfig, druidCoordinatorConfig);
  }

  private void validateConfiguration(
      SegmentsMetadataManagerConfig segmentsMetadataManagerConfig,
      DruidCoordinatorConfig druidCoordinatorConfig
  )
  {
    if (druidCoordinatorConfig.getCoordinatorKillOn() &&
        (null == druidCoordinatorConfig.getCoordinatorKillBufferPeriod() ||
         druidCoordinatorConfig.getCoordinatorKillBufferPeriod()
                               .isShorterThan(segmentsMetadataManagerConfig.getPollDuration()
                                                                           .toStandardDuration()
                                                                           .withDurationAdded(1000L, 1)))) {
      throw new ISE(
          "Invalid configuration: druid.manager.segments.pollingType=incremental requires "
          + "druid.coordinator.kill.bufferPeriod be set to a period larger than druid.manager.segments.pollDuration");
    }
  }

  private String getSegmentsTable()
  {
    return dbTables.get().getSegmentsTable();
  }

  @VisibleForTesting
  Set<DataSegment> mergeSegments(List<Segment> newList, ImmutableDruidDataSource dataSource)
  {
    if (log.isDebugEnabled()) {
      log.debug("merging %d found segments with %d existing segments for datasource %s",
                newList.size(), dataSource.getSegments().size(), dataSource.getName()
      );
    }
    Set<SegmentId> removedSegmentIds =
        newList.stream()
               .filter(s -> !s.used)
               .map(s -> s.segment.getId())
               .collect(Collectors.toCollection(HashSet::new));

    Set<DataSegment> result = new HashSet<>();
    // add new segments
    newList.stream()
           .filter(s -> s.used)
           .filter(s -> null == dataSource.getSegment(s.segment.getId()))
           .forEach(s -> result.add(s.segment));
    if (log.isDebugEnabled()) {
      log.debug("... %d segments removed, %d segments added", removedSegmentIds.size(), result.size());
    }

    // add old segments that were not removed
    dataSource.getSegments().stream().filter(s -> !removedSegmentIds.contains(s.getId())).forEach(result::add);
    if (log.isDebugEnabled()) {
      log.debug("Datasource %s now has %d used segments", dataSource.getName(), result.size());
    }
    return result;
  }

  /**
   * Creates a new snapshot by merging the previous snapshot with artifacts generated from the newly queried segments.
   *
   * @param previousSnapshot previous datasources snapshot; may be null if no previous snapshot exists
   * @return new snapshot
   */
  @Override
  public DataSourcesSnapshot create(@Nullable DataSourcesSnapshot previousSnapshot)
  {
    long start = System.currentTimeMillis();

    String asOfDate = DateTimes.nowUtc().toString(); // this must be set before querying segments!
    log.debug("new snapshot will have asOfDate = %s", asOfDate);
    QueryResult queryResult = querySegments(null == previousSnapshot ? null : previousSnapshot.getAsOfDate());

    final Map<String, List<Segment>> queriedSegmentsByDatasource =
        groupSegmentsByDatasource(queryResult.usedSegments, queryResult.unusedSegments);

    final Map<String, List<SegmentId>> oldOvershadowedByDatasource =
        groupOldOvershadowedByDatasource(previousSnapshot);

    final Map<String, ImmutableDruidDataSource> dataSources = new HashMap<>();
    final Map<String, VersionedIntervalTimeline<String, DataSegment>> timelines = new HashMap<>();
    final Set<SegmentId> overshadowedSegments = new HashSet<>();

    if (null != previousSnapshot) {
      previousSnapshot.getDataSourcesMap().forEach((dsName, ds) -> {
        List<Segment> newSegments = queriedSegmentsByDatasource.get(dsName);
        if (null == newSegments) {  // we received no new info for this datasource; it is unchanged
          // just copy data from old snapshot to new
          dataSources.put(dsName, ds);
          timelines.put(dsName, previousSnapshot.getUsedSegmentsTimelinesPerDataSource().get(dsName));
          List<SegmentId> oldOvershadowed = oldOvershadowedByDatasource.get(dsName);
          if (null != oldOvershadowed) {
            overshadowedSegments.addAll(oldOvershadowed);
          }
        } else {
          Set<DataSegment> mergedSegments = mergeSegments(newSegments, ds);
          if (!mergedSegments.isEmpty()) {
            buildDatasourceSnapshot(dataSources, timelines, overshadowedSegments, dsName, mergedSegments);
          }
        }
      });
    }

    // add newly discovered datasources which were not in the old snapshot
    queriedSegmentsByDatasource.forEach((dsName, segmentList) -> {
      if (!dataSources.containsKey(dsName)) {
        List<DataSegment> dataSegments = segmentList.stream()
                                                    .filter(s -> s.used)
                                                    .map(s -> s.segment)
                                                    .collect(Collectors.toList());
        if (!dataSegments.isEmpty()) {
          log.debug("Adding new datasource to snapshot: %s", dsName);
          buildDatasourceSnapshot(dataSources, timelines, overshadowedSegments, dsName, dataSegments);
        }
      }
    });

    log.info("Incrementally created DataSourcesSnapshot in %dms", System.currentTimeMillis() - start);
    return new DataSourcesSnapshot(dataSources, timelines, overshadowedSegments, asOfDate);
  }

  private Map<String, List<SegmentId>> groupOldOvershadowedByDatasource(DataSourcesSnapshot oldSnapshot)
  {
    return null == oldSnapshot ? Collections.emptyMap() :
           oldSnapshot.getOvershadowedSegments().stream().collect(
               Collectors.groupingBy(SegmentId::getDataSource));
  }

  private Map<String, List<Segment>> groupSegmentsByDatasource(
      Collection<DataSegment> usedSegments,
      Collection<DataSegment> unusedSegments
  )
  {
    return Stream.concat(
        usedSegments.stream().map(s1 -> new Segment(s1, true)),
        unusedSegments.stream().map(s2 -> new Segment(s2, false))
    ).collect(Collectors.groupingBy(s -> s.segment.getDataSource()));
  }

  /**
   * Updates the dataSources, timelines, and overshadowedSegments collections with artifacts for the
   * given datasource and segments.
   *
   * @param dataSources          datasource map to be updated
   * @param timelines            timelines collection to be updated
   * @param overshadowedSegments collection to be updated
   * @param dsName               name of the datasource
   * @param segments             the datasource's segments
   */
  private void buildDatasourceSnapshot(
      final Map<String, ImmutableDruidDataSource> dataSources,
      final Map<String, VersionedIntervalTimeline<String, DataSegment>> timelines,
      final Set<SegmentId> overshadowedSegments,
      final String dsName,
      final Collection<DataSegment> segments
  )
  {
    long start = System.currentTimeMillis();
    dataSources.put(dsName, new ImmutableDruidDataSource(
        dsName,
        createDefaultDataSourceProperties(),
        segments
    ));
    VersionedIntervalTimeline<String, DataSegment> timeline =
        VersionedIntervalTimeline.forSegments(segments);
    timelines.put(dsName, timeline);
    overshadowedSegments.addAll(
        segments.stream()
                .filter(s -> timeline.isOvershadowed(s.getInterval(), s.getVersion(), s))
                .map(DataSegment::getId)
                .collect(Collectors.toList()));
    log.info("Found overshadowed segments for datasource %s in %dms", dsName, System.currentTimeMillis() - start);
  }

  @VisibleForTesting
  QueryResult querySegments(@Nullable final String since)
  {
    // some databases such as PostgreSQL require auto-commit turned off
    // to stream results back, enabling transactions disables auto-commit
    //
    // setting connection to read-only will allow some database such as MySQL
    // to automatically use read-only transaction mode, further optimizing the query
    String sinceTime = null == since ? "1970-01-01" : since;
    log.debug("Looking for segments created/updated since " + sinceTime);
    // NOTE: some segments may have last_updated = NULL.  In standard ANSI SQL any comparison involving NULL is false.
    // This means that rows with NULL last_updated will never be selected by the last_updated >= :since check.
    // This is what we want, since any rows that have been updated will have a last_updated date.
    String query = StringUtils.format(
        "SELECT used, payload FROM %1$s WHERE created_date >= :since OR last_updated >= :since",
        getSegmentsTable()
    );
    final Collection<DataSegment> unusedSegments = new ArrayList<>();
    final Collection<DataSegment> usedSegments = new ArrayList<>();
    connector.inReadOnlyTransaction(
        (TransactionCallback<Void>) (handle, status) -> {
          try {
            handle
                .createQuery(query)
                .setFetchSize(connector.getStreamingFetchSize())
                .bind("since", sinceTime)
                .map(
                    (ResultSetMapper<Void>) (index, r, ctx) -> {
                      try {
                        DataSegment segment = jsonMapper.readValue(r.getBytes("payload"), DataSegment.class);
                        if (r.getBoolean("used")) {
                          usedSegments.add(segment);
                        } else {
                          unusedSegments.add(segment);
                        }
                      }
                      catch (IOException e) {
                        log.makeAlert(e, "Failed to read segment from db.").emit();
                        // If one entry in database is corrupted doPoll() should continue to work overall. See
                        // filter by `Objects::nonNull` below in this method.
                      }
                      return null;
                    }
                )
                .list();
          }
          catch (UnableToCreateStatementException e) {
            log.error(e, "Unable to do incremental select; last_updated column must exist on segments table "
                         + "in order to use incremental polling");
            throw e;
          }
          return null;
        }
    );
    if (log.isDebugEnabled()) {
      log.debug("Query found new/updated segments: %d used, %d unused", usedSegments.size(), unusedSegments.size());
    }
    return new QueryResult(usedSegments, unusedSegments);
  }

}
