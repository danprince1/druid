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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class AuditEventTest
{
  @Test
  public void testAuthenticationResultAuditEvent()
  {
    AuditEvent auditEvent = AuditEvent.builder()
                                      .setDimension("authenticator", "dummy")
                                      .setDimension("result", "PASS")
                                      .build("dummyUser", AuditEvent.EventType.AUTHENTICATION_RESULT)
                                      .build("service", "host");
    Assert.assertEquals(
        ImmutableMap.<String, Object>builder()
            .put("feed", "audit")
            .put("timestamp", auditEvent.getCreatedTime().toString())
            .put("service", "service")
            .put("host", "host")
            .put("identity", "dummyUser")
            .put("eventType", AuditEvent.EventType.AUTHENTICATION_RESULT.toString())
            .put("authenticator", "dummy")
            .put("result", "PASS")
            .build(),
        auditEvent.toMap()
    );
  }

  @Test
  public void testAuthorizationResultAuditEvent()
  {
    AuditEvent auditEvent = AuditEvent.builder()
              .setDimension("authorizer", "dummy")
              .setDimension("result", "FAIL")
              .build("dummyUser", AuditEvent.EventType.AUTHORIZATION_RESULT)
              .build("service", "host");

    Assert.assertEquals(
        ImmutableMap.<String, Object>builder()
            .put("feed", "audit")
            .put("timestamp", auditEvent.getCreatedTime().toString())
            .put("service", "service")
            .put("host", "host")
            .put("identity", "dummyUser")
            .put("eventType", AuditEvent.EventType.AUTHORIZATION_RESULT.toString())
            .put("authorizer", "dummy")
            .put("result", "FAIL")
            .build(),
        auditEvent.toMap()
    );
  }
}
