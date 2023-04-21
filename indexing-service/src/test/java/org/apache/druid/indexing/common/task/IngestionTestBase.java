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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.RegexInputFormat;
import org.apache.druid.data.input.impl.RegexParseSpec;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.SegmentInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9Factory;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.NoopDataSegmentKiller;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

public abstract class IngestionTestBase extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final TestUtils testUtils = new TestUtils();
  private final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
  private SegmentCacheManagerFactory segmentCacheManagerFactory;
  private TaskStorage taskStorage;
  private IndexerSQLMetadataStorageCoordinator storageCoordinator;
  private SegmentsMetadataManager segmentsMetadataManager;
  private TaskLockbox lockbox;
  private File taskReportsFile;

  @Before
  public void setUpIngestionTestBase() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    temporaryFolder.create();

    final SQLMetadataConnector connector = derbyConnectorRule.getConnector();
    connector.createTaskTables();
    connector.createSegmentTable();
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
    storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnectorRule.getConnector()
    );
    segmentsMetadataManager = new SqlSegmentsMetadataManager(
        objectMapper,
        SegmentsMetadataManagerConfig::new,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        derbyConnectorRule.getConnector()
    );
    lockbox = new TaskLockbox(taskStorage, storageCoordinator);
    segmentCacheManagerFactory = new SegmentCacheManagerFactory(getObjectMapper());
    taskReportsFile = temporaryFolder.newFile(
        StringUtils.format("ingestionTestBase-%s.json", System.currentTimeMillis())
    );
  }

  @After
  public void tearDownIngestionTestBase()
  {
    temporaryFolder.delete();
  }

  public TestLocalTaskActionClientFactory createActionClientFactory()
  {
    return new TestLocalTaskActionClientFactory();
  }

  public TestLocalTaskActionClient createActionClient(Task task)
  {
    return new TestLocalTaskActionClient(task);
  }

  public void prepareTaskForLocking(Task task) throws EntryExistsException
  {
    lockbox.add(task);
    taskStorage.insert(task, TaskStatus.running(task.getId()));
  }

  public void shutdownTask(Task task)
  {
    lockbox.remove(task);
  }

  public SegmentCacheManager newSegmentLoader(File storageDir)
  {
    return segmentCacheManagerFactory.manufacturate(storageDir);
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public TaskStorage getTaskStorage()
  {
    return taskStorage;
  }

  public File getTaskReportsFile()
  {
    return this.taskReportsFile;
  }

  public SegmentCacheManagerFactory getSegmentCacheManagerFactory()
  {
    return segmentCacheManagerFactory;
  }

  public IndexerMetadataStorageCoordinator getMetadataStorageCoordinator()
  {
    return storageCoordinator;
  }

  public SegmentsMetadataManager getSegmentsMetadataManager()
  {
    return segmentsMetadataManager;
  }

  public TaskLockbox getLockbox()
  {
    return lockbox;
  }

  public IndexerSQLMetadataStorageCoordinator getStorageCoordinator()
  {
    return storageCoordinator;
  }

  public RowIngestionMetersFactory getRowIngestionMetersFactory()
  {
    return testUtils.getRowIngestionMetersFactory();
  }

  public TaskActionToolbox createTaskActionToolbox()
  {
    storageCoordinator.start();
    return new TaskActionToolbox(
        lockbox,
        taskStorage,
        storageCoordinator,
        new NoopServiceEmitter(),
        null,
        objectMapper
    );
  }

  public IndexIO getIndexIO()
  {
    return testUtils.getTestIndexIO();
  }

  public IndexMergerV9Factory getIndexMergerV9Factory()
  {
    return testUtils.getIndexMergerV9Factory();
  }

  /**
   * Converts ParseSpec to InputFormat for indexing tests. To be used until {@link FirehoseFactory}
   * & {@link InputRowParser} is deprecated and removed.
   *
   * @param parseSpec
   * @return
   */
  public static InputFormat createInputFormatFromParseSpec(ParseSpec parseSpec)
  {
    if (parseSpec instanceof JSONParseSpec) {
      JSONParseSpec jsonParseSpec = (JSONParseSpec) parseSpec;
      return new JsonInputFormat(jsonParseSpec.getFlattenSpec(), jsonParseSpec.getFeatureSpec(), jsonParseSpec.getKeepNullColumns(), null, null);
    } else if (parseSpec instanceof CSVParseSpec) {
      CSVParseSpec csvParseSpec = (CSVParseSpec) parseSpec;
      boolean getColumnsFromHeader = csvParseSpec.isHasHeaderRow() && csvParseSpec.getSkipHeaderRows() == 0;
      return new CsvInputFormat(
          csvParseSpec.getColumns(),
          csvParseSpec.getListDelimiter(),
          getColumnsFromHeader ? null : true,
          getColumnsFromHeader ? true : null,
          csvParseSpec.getSkipHeaderRows()
      );
    } else if (parseSpec instanceof DelimitedParseSpec) {
      DelimitedParseSpec delimitedParseSpec = (DelimitedParseSpec) parseSpec;
      boolean getColumnsFromHeader = delimitedParseSpec.isHasHeaderRow() && delimitedParseSpec.getSkipHeaderRows() == 0;
      return new DelimitedInputFormat(
          delimitedParseSpec.getColumns(),
          delimitedParseSpec.getListDelimiter(),
          delimitedParseSpec.getDelimiter(),
          getColumnsFromHeader ? null : true,
          getColumnsFromHeader ? true : null,
          delimitedParseSpec.getSkipHeaderRows()
      );
    } else if (parseSpec instanceof RegexParseSpec) {
      RegexParseSpec regexParseSpec = (RegexParseSpec) parseSpec;
      return new RegexInputFormat(
          regexParseSpec.getPattern(),
          regexParseSpec.getListDelimiter(),
          regexParseSpec.getColumns());
    } else {
      throw new RE(StringUtils.format("Unsupported ParseSpec format %s", parseSpec.toString()));
    }
  }

  protected IngestionStatsAndErrorsTaskReportData getTaskReportData()
  {
    Map<String, TaskReport> taskReports = null;
    try {
      taskReports = objectMapper.readValue(
          getTaskReportsFile(),
          new TypeReference<Map<String, TaskReport>>()
          {
          }
      );
    }
    catch (IOException e) {
      Assert.fail("IOException when reading task reports");
    }
    return IngestionStatsAndErrorsTaskReportData.getPayloadFromTaskReports(
        taskReports
    );
  }

  protected void assertReportIntervalsEmpty()
  {
    Assert.assertTrue(getTaskReportData().getIngestedIntervals().isEmpty());
  }

  protected void checkReportIntervals(Collection<Interval> intervals)
  {
    Assert.assertEquals(ImmutableList.copyOf(intervals), getTaskReportData().getIngestedIntervals());
  }

  protected void checkReportInterval(@Nullable String interval)
  {
    if (null == interval) {
      assertReportIntervalsEmpty();
    } else {
      IngestionStatsAndErrorsTaskReportData payload = getTaskReportData();
      Assert.assertEquals(
          ImmutableList.of(Intervals.of(interval)),
          payload.getIngestedIntervals()
      );
    }
  }

  public class TestLocalTaskActionClientFactory implements TaskActionClientFactory
  {
    @Override
    public TaskActionClient create(Task task)
    {
      return new TestLocalTaskActionClient(task);
    }
  }

  public class TestLocalTaskActionClient extends CountingLocalTaskActionClientForTest
  {
    private final Set<DataSegment> publishedSegments = new HashSet<>();

    private TestLocalTaskActionClient(Task task)
    {
      super(task, taskStorage, createTaskActionToolbox());
    }

    @Override
    public <RetType> RetType submit(TaskAction<RetType> taskAction)
    {
      final RetType result = super.submit(taskAction);
      if (taskAction instanceof SegmentTransactionalInsertAction) {
        publishedSegments.addAll(((SegmentTransactionalInsertAction) taskAction).getSegments());
      } else if (taskAction instanceof SegmentInsertAction) {
        publishedSegments.addAll(((SegmentInsertAction) taskAction).getSegments());
      }
      return result;
    }

    public Set<DataSegment> getPublishedSegments()
    {
      return publishedSegments;
    }
  }

  public class TestTaskRunner implements TaskRunner
  {
    private TestLocalTaskActionClient taskActionClient;

    @Override
    public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void start()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerListener(TaskRunnerListener listener, Executor executor)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterListener(String listenerId)
    {
      throw new UnsupportedOperationException();
    }

    public TestLocalTaskActionClient getTaskActionClient()
    {
      return taskActionClient;
    }

    public List<DataSegment> getPublishedSegments()
    {
      final List<DataSegment> segments = new ArrayList<>(taskActionClient.getPublishedSegments());
      Collections.sort(segments);
      return segments;
    }

    @Override
    public ListenableFuture<TaskStatus> run(Task task)
    {
      try {
        lockbox.add(task);
        taskStorage.insert(task, TaskStatus.running(task.getId()));
        taskActionClient = createActionClient(task);

        final TaskConfig config = new TaskConfigBuilder()
            .setBatchProcessingMode(TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name())
            .build();
        final TaskToolbox box = new TaskToolbox.Builder()
            .config(config)
            .taskExecutorNode(new DruidNode("druid/middlemanager", "localhost", false, 8091, null, true, false))
            .taskActionClient(taskActionClient)
            .segmentPusher(new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig()))
            .dataSegmentKiller(new NoopDataSegmentKiller())
            .joinableFactory(NoopJoinableFactory.INSTANCE)
            .jsonMapper(objectMapper)
            .taskWorkDir(temporaryFolder.newFolder())
            .indexIO(getIndexIO())
            .indexMergerV9(testUtils.getIndexMergerV9Factory()
                                    .create(task.getContextValue(Tasks.STORE_EMPTY_COLUMNS_KEY, true)))
            .taskReportFileWriter(new SingleFileTaskReportFileWriter(taskReportsFile))
            .authorizerMapper(AuthTestUtils.TEST_AUTHORIZER_MAPPER)
            .chatHandlerProvider(new NoopChatHandlerProvider())
            .rowIngestionMetersFactory(testUtils.getRowIngestionMetersFactory())
            .appenderatorsManager(new TestAppenderatorsManager())
            .taskLogPusher(null)
            .attemptId("1")
            .build();


        if (task.isReady(box.getTaskActionClient())) {
          return Futures.immediateFuture(task.run(box));
        } else {
          throw new ISE("task is not ready");
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        lockbox.remove(task);
      }
    }

    @Override
    public void shutdown(String taskid, String reason)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stop()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ScalingStats> getScalingStats()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getTotalTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getIdleTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getUsedTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getLazyTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getBlacklistedTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }
  }
}
