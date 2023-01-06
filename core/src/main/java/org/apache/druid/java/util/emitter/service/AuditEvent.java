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

package org.apache.druid.java.util.emitter.service;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.TreeMap;

/**
 * AuditEvents are service,host level events containing security audit
 * information. An example, an authentication result for some identity X.
 */
@PublicApi
public class AuditEvent implements Event
{
  private final DateTime createdTime;
  private final ImmutableMap<String, String> serviceDimensions;
  private final Map<String, Object> userDims;
  private EventType eventType;
  private final String identity;

  private AuditEvent(
      DateTime createdTime,
      ImmutableMap<String, String> serviceDimensions,
      Map<String, Object> userDims,
      EventType eventType,
      String identity
  )
  {
    this.createdTime = createdTime != null ? createdTime : DateTimes.nowUtc();
    this.serviceDimensions = serviceDimensions;
    this.userDims = userDims;
    this.identity = identity;
    this.eventType = eventType;
  }

  public static AuditEvent.Builder builder()
  {
    return new AuditEvent.Builder();
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  @Override
  public String getFeed()
  {
    return "audit";
  }

  public String getService()
  {
    return serviceDimensions.get("service");
  }

  public String getHost()
  {
    return serviceDimensions.get("host");
  }

  public Map<String, Object> getUserDims()
  {
    return ImmutableMap.copyOf(serviceDimensions);
  }

  public String getIdentity()
  {
    return identity;
  }

  public EventType getEventType()
  {
    return eventType;
  }

  @Override
  @JsonValue
  public EventMap toMap()
  {
    return EventMap
        .builder()
        .put("feed", getFeed())
        .put("timestamp", createdTime.toString())
        .putAll(serviceDimensions)
        .put("identity", identity)
        .put("eventType", eventType.toString())
        .putAll(
            Maps.filterEntries(
                userDims,
                new Predicate<Map.Entry<String, Object>>()
                {
                  @Override
                  public boolean apply(Map.Entry<String, Object> input)
                  {
                    return input.getKey() != null;
                  }
                }
            )
        )
        .build();
  }

  public static class Builder
  {
    private final Map<String, Object> userDims = new TreeMap<>();

    public AuditEvent.Builder setDimension(String dim, Object value)
    {
      userDims.put(dim, value);
      return this;
    }

    public Object getDimension(String dim)
    {
      return userDims.get(dim);
    }

    public ServiceEventBuilder<AuditEvent> build(
        final String identity,
        final EventType eventType
    )
    {
      return build(null, identity, eventType);
    }

    public ServiceEventBuilder<AuditEvent> build(
        final DateTime createdTime,
        final String identity,
        final EventType eventType
    )
    {
      return new ServiceEventBuilder<AuditEvent>()
      {
        @Override
        public AuditEvent build(ImmutableMap<String, String> serviceDimensions)
        {
          return new AuditEvent(
              createdTime,
              serviceDimensions,
              userDims,
              eventType,
              identity
          );
        }
      };
    }
  }

  public enum EventType
  {
    AUTHENTICATION_RESULT {
      @Override
      public String toString()
      {
        return "authentication-result";
      }
    },
    AUTHORIZATION_RESULT {
      @Override
      public String toString()
      {
        return "authorization-result";
      }
    },
    CONFIG_WRITE {
      @Override
      public String toString()
      {
        return "config-write";
      }
    };
  }
}
