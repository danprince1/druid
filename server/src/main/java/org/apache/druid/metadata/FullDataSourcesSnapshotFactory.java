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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DataSourcesSnapshotFactory;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * Builds a new {@link DataSourcesSnapshot} by selecting all segments from metadata on each poll.
 *
 * @see IncrementalDataSourcesSnapshotFactory
 */
public class FullDataSourcesSnapshotFactory implements DataSourcesSnapshotFactory
{
  private static final EmittingLogger log = new EmittingLogger(FullDataSourcesSnapshotFactory.class);

  private static ImmutableMap<String, String> createDefaultDataSourceProperties()
  {
    return ImmutableMap.of("created", DateTimes.nowUtc().toString());
  }

  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final ObjectMapper jsonMapper;
  private final SQLMetadataConnector connector;

  @Inject
  public FullDataSourcesSnapshotFactory(
      Supplier<MetadataStorageTablesConfig> dbTables,
      ObjectMapper jsonMapper,
      SQLMetadataConnector connector
  )
  {
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.connector = connector;
  }

  private String getSegmentsTable()
  {
    return dbTables.get().getSegmentsTable();
  }

  /**
   * For the garbage collector in Java, it's better to keep new objects short-living, but once they are old enough
   * (i.e. promoted to old generation), try to keep them alive. When querying metadata, we fetch and deserialize all
   * existing segments each time, and then replace them in the {@link DataSourcesSnapshot}. This method allows us to use
   * already existing (old) segments when possible, effectively interning them a-la {@link String#intern} or {@link
   * com.google.common.collect.Interner}, aiming to make the majority of {@link DataSegment} objects garbage soon after
   * they are deserialized and to die in young generation. It allows us to avoid fragmentation of the old generation and
   * full GCs.
   */
  private DataSegment replaceWithExistingSegmentIfPresent(DataSegment segment, DataSourcesSnapshot dataSourcesSnapshot)
  {
    if (dataSourcesSnapshot == null) {
      return segment;
    }
    @Nullable
    ImmutableDruidDataSource dataSource = dataSourcesSnapshot.getDataSource(segment.getDataSource());
    if (dataSource == null) {
      return segment;
    }
    DataSegment alreadyExistingSegment = dataSource.getSegment(segment.getId());
    return alreadyExistingSegment != null ? alreadyExistingSegment : segment;
  }

  @Override
  public DataSourcesSnapshot create(@Nullable DataSourcesSnapshot previousSnapshot)
  {
    // some databases such as PostgreSQL require auto-commit turned off
    // to stream results back, enabling transactions disables auto-commit
    //
    // setting connection to read-only will allow some database such as MySQL
    // to automatically use read-only transaction mode, further optimizing the query
    final List<DataSegment> segments = connector.inReadOnlyTransaction(
        new TransactionCallback<List<DataSegment>>()
        {
          @Override
          public List<DataSegment> inTransaction(Handle handle, TransactionStatus status)
          {
            return handle
                .createQuery(StringUtils.format("SELECT payload FROM %s WHERE used=true", getSegmentsTable()))
                .setFetchSize(connector.getStreamingFetchSize())
                .map(
                    new ResultSetMapper<DataSegment>()
                    {
                      @Override
                      public DataSegment map(int index, ResultSet r, StatementContext ctx) throws SQLException
                      {
                        try {
                          DataSegment segment = jsonMapper.readValue(r.getBytes("payload"), DataSegment.class);
                          return replaceWithExistingSegmentIfPresent(segment, previousSnapshot);
                        }
                        catch (IOException e) {
                          log.makeAlert(e, "Failed to read segment from db.").emit();
                          // If one entry in database is corrupted doPoll() should continue to work overall. See
                          // filter by `Objects::nonNull` below in this method.
                          return null;
                        }
                      }
                    }
                )
                .list();
          }
        }
    );


    Preconditions.checkNotNull(
        segments,
        "Unexpected 'null' when polling segments from the db, aborting snapshot update."
    );

    ImmutableMap<String, String> dataSourceProperties = createDefaultDataSourceProperties();
    if (segments.isEmpty()) {
      log.info("No segments found in the database!");
    } else {
      log.info("Polled and found %,d segments in the database", segments.size());
    }
    return DataSourcesSnapshot.fromUsedSegments(
        Iterables.filter(segments, Objects::nonNull), // Filter corrupted entries (see above in this method).
        dataSourceProperties
    );
  }

}
