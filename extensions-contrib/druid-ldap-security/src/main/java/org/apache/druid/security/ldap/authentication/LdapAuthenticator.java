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

package org.apache.druid.security.ldap.authentication;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;
import java.util.Map;

@JsonTypeName("ldap")
public class LdapAuthenticator implements Authenticator
{
  private static final Logger log = new Logger(LdapAuthenticator.class);

  private final String name;
  private final String authorizerName;
  private final String lookupFile;
  private final int clientCredentialsPoll;
  private final boolean skipOnFailure;
  private final ServiceEmitter emitter;

  @JsonCreator
  public LdapAuthenticator(
      @JsonProperty("name") String name,
      @JsonProperty("authorizerName") String authorizerName,
      @JsonProperty("lookupFile") String lookupFile,
      @JsonProperty("clientCredentialsPoll") int clientCredentialsPoll,
      @JsonProperty("skipOnFailure") Boolean skipOnFailure,
      @JacksonInject ServiceEmitter emitter
  )
  {
    this.name = name;
    this.authorizerName = authorizerName;
    this.lookupFile = lookupFile;
    this.clientCredentialsPoll = clientCredentialsPoll;
    this.skipOnFailure = skipOnFailure == null ? false : skipOnFailure;
    this.emitter = emitter;
  }

  @Override
  public Filter getFilter()
  {
    return new LdapAuthenticationFilter(this.authorizerName, this.lookupFile, this.clientCredentialsPoll, skipOnFailure, emitter);
  }

  @Override
  public String getAuthChallengeHeader()
  {
    return "Basic";
  }

  @Override
  @Nullable
  public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
  {
    // This method shouldn't being called
    log.info("----------- Why authenticateJDBCContext is being called!!??");
    return null;
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return LdapAuthenticationFilter.class;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    return null;
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }

}
