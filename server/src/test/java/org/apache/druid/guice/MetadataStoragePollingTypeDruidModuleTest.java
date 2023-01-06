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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import org.apache.druid.client.DataSourcesSnapshotFactory;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.emitter.core.LoggingEmitter;
import org.apache.druid.java.util.emitter.core.LoggingEmitterConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.FullDataSourcesSnapshotFactory;
import org.apache.druid.metadata.IncrementalDataSourcesSnapshotFactory;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorageDruidModule;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.emitter.LogEmitterModule;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;


public class MetadataStoragePollingTypeDruidModuleTest
{

  static class TestModule extends AbstractModule
  {
    private final Properties properties;

    public TestModule()
    {
      this.properties = new Properties();
    }

    TestModule(Properties properties)
    {
      this.properties = properties;
    }

    @Override
    protected void configure()
    {
      install(new DruidGuiceExtensions());
      install(new ConfigModule());
      install(new JacksonModule());
      install(new LogEmitterModule());
      install(new LifecycleModule());
      install(new MetadataConfigModule());
      install(new DerbyMetadataStorageDruidModule());
      install(new MetadataStoragePollingTypeDruidModule());

      binder().bind(DruidNode.class).annotatedWith(Self.class).toInstance(
          new DruidNode("integration-tests", "localhost", false, 9191, null, null, true, false)
      );

      binder().bind(Properties.class).toInstance(this.properties);
    }

    @Provides
    @ManageLifecycle
    public ServiceEmitter getServiceEmitter(Supplier<LoggingEmitterConfig> config, ObjectMapper jsonMapper)
    {
      return new ServiceEmitter("", "", new LoggingEmitter(config.get(), jsonMapper));
    }
  }

  @Test
  public void testDefault()
  {
    Injector injector = Guice.createInjector(new TestModule());

    DataSourcesSnapshotFactory factory = injector.getInstance(DataSourcesSnapshotFactory.class);

    Assert.assertTrue(factory instanceof FullDataSourcesSnapshotFactory);
  }

  @Test
  public void testIncremental()
  {
    Properties properties = new Properties();
    properties.setProperty(MetadataStoragePollingTypeDruidModule.PROPERTY, "incremental");

    Injector injector = Guice.createInjector(new TestModule(properties));

    DataSourcesSnapshotFactory factory = injector.getInstance(DataSourcesSnapshotFactory.class);

    Assert.assertTrue(factory instanceof IncrementalDataSourcesSnapshotFactory);
  }

  @Test
  public void testFull()
  {
    Properties properties = new Properties();
    properties.setProperty(MetadataStoragePollingTypeDruidModule.PROPERTY, "full");

    Injector injector = Guice.createInjector(new TestModule(properties));

    DataSourcesSnapshotFactory factory = injector.getInstance(DataSourcesSnapshotFactory.class);

    Assert.assertTrue(factory instanceof FullDataSourcesSnapshotFactory);
  }
}
