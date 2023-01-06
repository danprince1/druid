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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.IncrementalDataSourcesSnapshotFactory.QueryResult;
import org.apache.druid.metadata.IncrementalDataSourcesSnapshotFactory.Segment;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


@RunWith(MockitoJUnitRunner.class)
public class IncrementalDataSourcesSnapshotFactoryTest
{
  private static final String DATASOURCE_NAME = "MyDatasource";
  private static final String DATASOURCE2_NAME = "MyDatasource2";

  final DataSegment dataSegment1 = createSegment("2020-01-01/2020-01-02");
  final DataSegment dataSegment2 = createSegment("2020-01-02/2020-01-03");
  final DataSegment dataSegment3 = createSegment("2020-01-03/2020-01-04");
  // this segment fully overshadows segment3
  final DataSegment dataSegment4 = createSegment("2020-01-02T10:00:00/2020-01-04T18:00:00", "2");
  final DataSegment dataSegment5 = createSegment("2020-01-03/2020-01-04", "1", DATASOURCE2_NAME);

  @Mock
  private Supplier<MetadataStorageTablesConfig> dbTables;
  @Mock
  private ObjectMapper jsonMapper;
  @Mock
  private SQLMetadataConnector connector;
  @Mock
  private SegmentsMetadataManagerConfig segmentsMetadataManagerConfig;
  @Mock
  private DruidCoordinatorConfig druidCoordinatorConfig;
  private IncrementalDataSourcesSnapshotFactory factory;

  @Before
  public void setUp() throws Exception
  {
    Mockito.doReturn(true).when(druidCoordinatorConfig).getCoordinatorKillOn();
    Mockito.doReturn(Duration.parse("PT3600S")).when(druidCoordinatorConfig).getCoordinatorKillBufferPeriod();
    Mockito.doReturn(Period.parse("PT1S")).when(segmentsMetadataManagerConfig).getPollDuration();
    factory = Mockito.spy(new IncrementalDataSourcesSnapshotFactory(
        dbTables,
        jsonMapper,
        connector,
        segmentsMetadataManagerConfig,
        druidCoordinatorConfig
    ));
  }

  private DataSegment createSegment(String interval)
  {
    return createSegment(interval, "1");
  }

  private DataSegment createSegment(String interval, String version)
  {
    return createSegment(interval, version, DATASOURCE_NAME);
  }

  private DataSegment createSegment(String interval, String version, String dataSourceName)
  {
    return DataSegment.builder()
                      .dataSource(dataSourceName)
                      .interval(Intervals.of(interval))
                      .version(version)
                      .size(3)
                      .build();
  }

  private void assertContainsSame(Collection<DataSegment> segments, DataSegment segment)
  {
    DataSegment found =
        segments.stream().filter(s -> s.getInterval().equals(segment.getInterval())).findFirst().orElse(null);
    Assert.assertSame(segment, found);
  }

  private void assertBasics(DataSourcesSnapshot snapshot)
  {
    ImmutableDruidDataSource dataSource = snapshot.getDataSource(DATASOURCE_NAME);
    Assert.assertEquals(1, snapshot.getDataSourcesMap().size());
    Assert.assertNotNull(dataSource);
    Assert.assertEquals(DATASOURCE_NAME, dataSource.getName());
    Assert.assertEquals(1, dataSource.getProperties().size());
    Assert.assertEquals("created", dataSource.getProperties().keySet().iterator().next());
    Assert.assertEquals(1, snapshot.getUsedSegmentsTimelinesPerDataSource().size());
    Assert.assertEquals(DATASOURCE_NAME, snapshot.getUsedSegmentsTimelinesPerDataSource().keySet().iterator().next());
  }

  @Test
  public void testMergeSegments()
  {
    ImmutableDruidDataSource dataSource = new ImmutableDruidDataSource(
        DATASOURCE_NAME,
        Collections.emptyMap(),
        ImmutableList.of(
            dataSegment1,
            dataSegment2
        )
    );
    Segment segment1 = new Segment(dataSegment1, true);
    Segment segment2 = new Segment(dataSegment2, false);
    Segment segment3 = new Segment(dataSegment3, true);

    Set<DataSegment> mergedSegments = factory.mergeSegments(ImmutableList.of(
        segment1,
        segment2,
        segment3
    ), dataSource);

    Assert.assertSame(
        dataSegment1,
        mergedSegments.stream()
                      .filter(s -> s.getInterval().equals(Intervals.of("2020-01-01/2020-01-02")))
                      .findFirst()
                      .orElse(null)
    );
    Assert.assertTrue(mergedSegments.contains(dataSegment3));
  }

  @Test
  public void testMergeSegmentsEmptyNewList()
  {
    Set<DataSegment> originalSegments = ImmutableSet.of(
        dataSegment1,
        dataSegment2,
        dataSegment3
    );
    ImmutableDruidDataSource dataSource = new ImmutableDruidDataSource(
        DATASOURCE_NAME,
        Collections.emptyMap(),
        originalSegments
    );

    Set<DataSegment> segments = factory.mergeSegments(Collections.emptyList(), dataSource);

    Assert.assertEquals(originalSegments, segments);
  }

  @Test
  public void testCreateNewDatasource()
  {
    Set<DataSegment> originalSegments = ImmutableSet.of(
        dataSegment1,
        dataSegment2,
        dataSegment3
    );
    DataSourcesSnapshot oldSnapshot = new DataSourcesSnapshot(Collections.emptyMap());
    Mockito.doReturn(new QueryResult(originalSegments, Collections.emptyList())).when(factory).querySegments(
        ArgumentMatchers.any());
    DataSourcesSnapshot snapshot = factory.create(oldSnapshot);

    assertBasics(snapshot);
    ImmutableDruidDataSource dataSource = snapshot.getDataSource(DATASOURCE_NAME);
    Assert.assertNotNull(dataSource);
    Assert.assertEquals(3, dataSource.getSegments().size());
    assertContainsSame(dataSource.getSegments(), dataSegment1);
    assertContainsSame(dataSource.getSegments(), dataSegment2);
    assertContainsSame(dataSource.getSegments(), dataSegment3);
    Assert.assertEquals(9, dataSource.getTotalSizeOfSegments());
    Assert.assertTrue(snapshot.getOvershadowedSegments().isEmpty());
  }

  @Test
  public void testUnchangedDatasource()
  {
    Set<DataSegment> originalSegments = ImmutableSet.of(
        dataSegment1,
        dataSegment2,
        dataSegment3
    );
    ImmutableDruidDataSource oldDataSource =
        new ImmutableDruidDataSource(
            DATASOURCE_NAME,
            ImmutableMap.of("created", DateTimes.nowUtc().toString()),
            originalSegments
        );
    Map<String, ImmutableDruidDataSource> existingMap = ImmutableMap.of(DATASOURCE_NAME, oldDataSource);
    DataSourcesSnapshot oldSnapshot = new DataSourcesSnapshot(existingMap);
    Mockito.doReturn(new QueryResult(Collections.emptyList(), Collections.emptyList())).when(factory).querySegments(
        ArgumentMatchers.any());

    DataSourcesSnapshot snapshot = factory.create(oldSnapshot);

    assertBasics(snapshot);
    ImmutableDruidDataSource dataSource = snapshot.getDataSource(DATASOURCE_NAME);
    Assert.assertNotNull(dataSource);
    assertContainsSame(dataSource.getSegments(), dataSegment1);
    assertContainsSame(dataSource.getSegments(), dataSegment2);
    assertContainsSame(dataSource.getSegments(), dataSegment3);
    Assert.assertEquals(9, dataSource.getTotalSizeOfSegments());
    Assert.assertTrue(snapshot.getOvershadowedSegments().isEmpty());
  }

  @Test
  public void testCreateFirstSnapshot()
  {
    Set<DataSegment> originalSegments = ImmutableSet.of(
        dataSegment1,
        dataSegment2,
        dataSegment3
    );
    Mockito.doReturn(new QueryResult(originalSegments, Collections.emptyList())).when(factory).querySegments(
        ArgumentMatchers.any());

    DataSourcesSnapshot snapshot = factory.create(null);

    assertBasics(snapshot);
    ImmutableDruidDataSource dataSource = snapshot.getDataSource(DATASOURCE_NAME);
    Assert.assertNotNull(dataSource);
    assertContainsSame(dataSource.getSegments(), dataSegment1);
    assertContainsSame(dataSource.getSegments(), dataSegment2);
    assertContainsSame(dataSource.getSegments(), dataSegment3);
    Assert.assertEquals(9, dataSource.getTotalSizeOfSegments());
    Assert.assertTrue(snapshot.getOvershadowedSegments().isEmpty());
  }

  @Test
  public void testCreateSegmentUsedToUnusedToUsed()
  {
    Set<DataSegment> originalSegments = ImmutableSet.of(
        dataSegment1,
        dataSegment2,
        dataSegment3
    );
    ImmutableDruidDataSource oldDataSource =
        new ImmutableDruidDataSource(
            DATASOURCE_NAME,
            ImmutableMap.of("created", DateTimes.nowUtc().toString()),
            originalSegments
        );
    Map<String, ImmutableDruidDataSource> existingMap = ImmutableMap.of(DATASOURCE_NAME, oldDataSource);
    DataSourcesSnapshot oldSnapshot = new DataSourcesSnapshot(existingMap);

    final DataSegment foundDataSegment1 = createSegment("2020-01-01/2020-01-02");
    final DataSegment foundDataSegment2 = createSegment("2020-01-02/2020-01-03");
    Collection<DataSegment> used = new ArrayList<>();
    used.add(foundDataSegment1);
    used.add(foundDataSegment2);
    final DataSegment foundDataSegment3 = createSegment("2020-01-03/2020-01-04");
    Collection<DataSegment> unused = new ArrayList<>();
    unused.add(foundDataSegment3);
    Mockito.doReturn(new QueryResult(used, unused)).when(factory).querySegments(ArgumentMatchers.any());

    DataSourcesSnapshot snapshot = factory.create(oldSnapshot);

    assertBasics(snapshot);
    ImmutableDruidDataSource dataSource = snapshot.getDataSource(DATASOURCE_NAME);
    Assert.assertNotNull(dataSource);
    assertContainsSame(dataSource.getSegments(), dataSegment1);
    assertContainsSame(dataSource.getSegments(), dataSegment2);
    Assert.assertEquals(6, dataSource.getTotalSizeOfSegments());

    used.add(foundDataSegment3);
    unused.clear();
    Mockito.doReturn(new QueryResult(used, unused)).when(factory).querySegments(ArgumentMatchers.any());

    DataSourcesSnapshot thirdSnapshot = factory.create(snapshot);

    assertBasics(snapshot);
    dataSource = thirdSnapshot.getDataSource(DATASOURCE_NAME);
    Assert.assertNotNull(dataSource);
    Assert.assertEquals(3, dataSource.getSegments().size());
    assertContainsSame(dataSource.getSegments(), dataSegment1);
    assertContainsSame(dataSource.getSegments(), dataSegment2);
    Assert.assertTrue(dataSource.getSegments().contains(dataSegment3)); // note: not same
    Assert.assertEquals(9, dataSource.getTotalSizeOfSegments());
    Assert.assertTrue(snapshot.getOvershadowedSegments().isEmpty());
  }

  @Test
  public void testCreateOvershadowed()
  {
    Set<DataSegment> originalSegments = ImmutableSet.of(
        dataSegment1,
        dataSegment2,
        dataSegment3
    );
    ImmutableDruidDataSource oldDataSource =
        new ImmutableDruidDataSource(
            DATASOURCE_NAME,
            ImmutableMap.of("created", DateTimes.nowUtc().toString()),
            originalSegments
        );
    Map<String, ImmutableDruidDataSource> existingMap = ImmutableMap.of(DATASOURCE_NAME, oldDataSource);
    DataSourcesSnapshot oldSnapshot = new DataSourcesSnapshot(existingMap);

    Collection<DataSegment> used = ImmutableList.of(dataSegment4);
    Collection<DataSegment> unused = ImmutableList.of();
    Mockito.doReturn(new QueryResult(used, unused)).when(factory).querySegments(ArgumentMatchers.any());

    DataSourcesSnapshot snapshot = factory.create(oldSnapshot);

    assertBasics(snapshot);
    ImmutableDruidDataSource dataSource = snapshot.getDataSource(DATASOURCE_NAME);
    Assert.assertNotNull(dataSource);
    assertContainsSame(dataSource.getSegments(), dataSegment1);
    assertContainsSame(dataSource.getSegments(), dataSegment2);
    assertContainsSame(dataSource.getSegments(), dataSegment3);
    assertContainsSame(dataSource.getSegments(), dataSegment4);
    Assert.assertEquals(12, dataSource.getTotalSizeOfSegments());
    Assert.assertEquals(1, snapshot.getOvershadowedSegments().size());
    Assert.assertEquals(dataSegment3.getId(), snapshot.getOvershadowedSegments().iterator().next());
  }

  @Test
  public void testCreateSecondDatasource()
  {
    Set<DataSegment> originalSegments = ImmutableSet.of(
        dataSegment1,
        dataSegment2
    );
    ImmutableDruidDataSource oldDataSource =
        new ImmutableDruidDataSource(
            DATASOURCE_NAME,
            ImmutableMap.of("created", DateTimes.nowUtc().toString()),
            originalSegments
        );
    Map<String, ImmutableDruidDataSource> existingMap = ImmutableMap.of(DATASOURCE_NAME, oldDataSource);
    DataSourcesSnapshot oldSnapshot = new DataSourcesSnapshot(existingMap);

    Collection<DataSegment> used = ImmutableList.of(dataSegment5);
    Collection<DataSegment> unused = ImmutableList.of();
    Mockito.doReturn(new QueryResult(used, unused)).when(factory).querySegments(ArgumentMatchers.any());

    DataSourcesSnapshot snapshot = factory.create(oldSnapshot);

    Assert.assertEquals(2, snapshot.getUsedSegmentsTimelinesPerDataSource().size());
    Assert.assertTrue(snapshot.getUsedSegmentsTimelinesPerDataSource().containsKey(DATASOURCE_NAME));
    Assert.assertTrue(snapshot.getUsedSegmentsTimelinesPerDataSource().containsKey(DATASOURCE2_NAME));

    Assert.assertEquals(2, snapshot.getDataSourcesMap().size());
    ImmutableDruidDataSource dataSource1 = snapshot.getDataSource(DATASOURCE_NAME);
    Assert.assertNotNull(dataSource1);
    Assert.assertEquals(DATASOURCE_NAME, dataSource1.getName());
    Assert.assertEquals(1, dataSource1.getProperties().size());
    Assert.assertEquals("created", dataSource1.getProperties().keySet().iterator().next());
    Assert.assertEquals(2, dataSource1.getSegments().size());
    assertContainsSame(dataSource1.getSegments(), dataSegment1);
    assertContainsSame(dataSource1.getSegments(), dataSegment2);


    ImmutableDruidDataSource dataSource2 = snapshot.getDataSource(DATASOURCE2_NAME);
    Assert.assertNotNull(dataSource2);
    Assert.assertEquals(1, dataSource2.getSegments().size());
    assertContainsSame(dataSource2.getSegments(), dataSegment5);
    Assert.assertEquals(3, dataSource2.getTotalSizeOfSegments());
    Assert.assertEquals(0, snapshot.getOvershadowedSegments().size());
  }
}
