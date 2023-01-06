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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.auth.Credentials;
import org.apache.druid.security.ldap.LdapAuthUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;
import org.eclipse.jetty.client.api.Request;
import org.jboss.netty.handler.codec.http.HttpHeaders;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

@JsonTypeName("ldap")
public class LdapEscalator implements Escalator
{
  private static final Logger log = new Logger(LdapEscalator.class);

  private final String authorizerName;
  private final String internalClientCredentialFile;
  private final int internalClientCredentialPoll;

  private final AtomicReference<LdapEscalatorConfig> configHolder = new AtomicReference<>();

  @JsonCreator
  public LdapEscalator(
          @JsonProperty("authorizerName") String authorizerName,
          @JsonProperty("internalClientCredentialFile") String internalClientCredentialFile,
          @JsonProperty("internalClientCredentialPoll") int internalClientCredentialPoll
  )
  {
    log.debug("----------- Initializing LdapEscalator");
    this.authorizerName = authorizerName;
    this.internalClientCredentialFile = internalClientCredentialFile;
    this.internalClientCredentialPoll = internalClientCredentialPoll;

    configHolder.set(loadConfiguration());
  }

  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    log.debug("----------- Creating escalated client");
    return new CredentialedHttpClient(new LdapEscalatorCredentials(), baseClient);
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    LdapEscalatorConfig ldapEscalatorConfig = getOrUpdateConfiguration();
    log.debug("----------- Creating escalated authentication result. username: %s", ldapEscalatorConfig);
    return ldapEscalatorConfig.createAuthenticationResult();
  }

  private LdapEscalatorConfig getOrUpdateConfiguration()
  {
    long now = System.currentTimeMillis();
    long cutoff = now - (internalClientCredentialPoll * 1000L);

    LdapEscalatorConfig config = configHolder.get();
    if (config.getLastVerified() < cutoff) {
      File file = new File(this.internalClientCredentialFile);
      if (config.getLastModified() != file.lastModified()) {
        log.debug("----------- Modified escalator credentials found, reloading");
        config = loadConfiguration();
        configHolder.set(config);
      } else {
        log.debug("----------- Escalator credentials validated, updating verification time");
        config = new LdapEscalatorConfig(config, now);
        configHolder.set(config);
      }
    }
    return config;
  }

  private final class LdapEscalatorCredentials implements Credentials
  {
    @Override
    public org.apache.druid.java.util.http.client.Request addCredentials(org.apache.druid.java.util.http.client.Request builder)
    {
      LdapEscalatorConfig config = getOrUpdateConfiguration();
      log.debug("----------- Adding authenticated credentials. username: %s", config);
      return builder.setBasicAuthentication(config.username, config.password);
    }
  }

  private LdapEscalatorConfig loadConfiguration()
  {
    try {
      log.debug("----------- Loading LDAP escalator credential file: %s", internalClientCredentialFile);
      File file = new File(this.internalClientCredentialFile);
      long lastModified = file.lastModified();

      String internalClientCredential = Files.readFirstLine(file, StandardCharsets.UTF_8);
      if (internalClientCredential == null) {
        throw new IllegalArgumentException("Missing escalator credentials");
      }

      String[] parts = internalClientCredential.split(":");
      if (parts.length < 2) {
        throw new IllegalArgumentException("Malformed escalator credentials found");
      }

      String username = parts[0];
      String password = parts[1];

      return new LdapEscalatorConfig(username, password, lastModified, System.currentTimeMillis());
    }
    catch (IOException e) {
      log.error(e, "Unable to read escalator configuration");
      Throwables.propagate(e);
      return null;
    }
  }

  private class LdapEscalatorConfig
  {
    private final String username;
    private final String password;
    private final long lastModified;
    private final long lastVerified;

    public LdapEscalatorConfig(String username, String password, long lastModified, long lastVerified)
    {
      this.username = username;
      this.password = password;
      this.lastModified = lastModified;
      this.lastVerified = lastVerified;
    }

    public LdapEscalatorConfig(LdapEscalatorConfig current, long lastVerified)
    {
      this.username = current.username;
      this.password = current.password;
      this.lastModified = current.lastModified;
      this.lastVerified = lastVerified;
    }

    public BasicCredentials getCredentials()
    {
      return new BasicCredentials(username, password);
    }

    public void updateRequest(Request request)
    {
      final String unencodedCreds = StringUtils.format("%s:%s", username, password);
      final String base64Creds = LdapAuthUtils.getEncodedCredentials(unencodedCreds);
      request.getHeaders().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + base64Creds);
    }

    public AuthenticationResult createAuthenticationResult()
    {
      return new AuthenticationResult(username, authorizerName, null, null);

    }

    public long getLastModified()
    {
      return lastModified;
    }

    public long getLastVerified()
    {
      return lastVerified;
    }

    @Override
    public String toString()
    {
      return username;
    }
  }
}
