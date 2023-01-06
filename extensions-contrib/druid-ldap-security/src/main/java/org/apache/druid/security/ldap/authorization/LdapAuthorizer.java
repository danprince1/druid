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

package org.apache.druid.security.ldap.authorization;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.emitter.service.AuditEvent;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;

@JsonTypeName("ldap")
public class LdapAuthorizer implements Authorizer
{
  private final String name;
  private final ServiceEmitter emitter;

  @JsonCreator
  public LdapAuthorizer(
      @JsonProperty("name") String name,
      @JacksonInject ServiceEmitter emitter
  )
  {
    this.name = name;
    this.emitter = emitter;
  }

  @Override
  public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
  {
    if (authenticationResult == null) {
      throw new IAE("authenticationResult should never be null.");
    }

    emitter.emit(
        new AuditEvent.Builder()
            .setDimension("authorizer", name)
            .setDimension("result", "PASS")
            .setDimension("action", action.toString())
            .setDimension("resourceName", resource.getName())
            .setDimension("resourceType", resource.getType())
            .build(authenticationResult.getIdentity(), AuditEvent.EventType.AUTHORIZATION_RESULT)
    );
    return Access.OK;
  }
}
