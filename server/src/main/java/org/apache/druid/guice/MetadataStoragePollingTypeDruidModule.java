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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.client.DataSourcesSnapshotFactory;
import org.apache.druid.metadata.FullDataSourcesSnapshotFactory;
import org.apache.druid.metadata.IncrementalDataSourcesSnapshotFactory;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;

public class MetadataStoragePollingTypeDruidModule implements Module
{
  public static final String PROPERTY = "druid.manager.segments.pollingType";

  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, DruidCoordinatorConfig.class);

    PolyBind.createChoiceWithDefault(binder, PROPERTY, Key.get(DataSourcesSnapshotFactory.class),
                                     SegmentsMetadataManagerConfig.PollingType.full.name()
    );

    PolyBind.optionBinder(binder, Key.get(DataSourcesSnapshotFactory.class))
            .addBinding(SegmentsMetadataManagerConfig.PollingType.full.name())
            .to(FullDataSourcesSnapshotFactory.class);

    PolyBind.optionBinder(binder, Key.get(DataSourcesSnapshotFactory.class))
            .addBinding(SegmentsMetadataManagerConfig.PollingType.incremental.name())
            .to(IncrementalDataSourcesSnapshotFactory.class);
  }
}
