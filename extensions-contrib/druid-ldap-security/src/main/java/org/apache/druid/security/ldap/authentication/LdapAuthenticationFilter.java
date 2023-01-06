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

import com.google.common.io.Files;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.AuditEvent;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.security.ldap.LdapAuthUtils;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LdapAuthenticationFilter implements Filter
{
  private static final Logger log = new Logger(LdapAuthenticationFilter.class);

  private final String authorizerName;
  private final String lookupFile;
  private final int clientCredentialsPoll;
  private final ClientCredentialsThread thread;
  private final boolean skipOnFailure;
  private final ServiceEmitter emitter;

  private final AtomicReference<Set<String>> clientCredentials = new AtomicReference<>();

  public LdapAuthenticationFilter(
      String authorizerName,
      String lookupFile,
      int clientCredentialsPoll,
      boolean skipOnFailure,
      ServiceEmitter emitter
  )
  {
    log.info("----------- Constructing LdapAuthenticationFilter");
    this.authorizerName = authorizerName;
    this.lookupFile = lookupFile;
    this.clientCredentialsPoll = clientCredentialsPoll;
    this.thread = new ClientCredentialsThread(lookupFile, clientCredentialsPoll, clientCredentials);
    this.skipOnFailure = skipOnFailure;
    this.emitter = emitter;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException
  {
    log.info("----------- Initializing LdapAuthenticationFilter");
    File file = new File(this.lookupFile);
    try {
      log.info("----------- Loading client credentials file: %s", this.lookupFile);
      loadClientCredentials(file, clientCredentials);
      thread.start();
    }
    catch (IOException e) {
      log.error(e, "----------- Error loading LDAP client credentials file: %s", lookupFile);
      throw new ServletException("Exception encountered loading ldap lookup file", e);
    }
  }

  @Override
  public void doFilter(
      ServletRequest servletRequest,
      ServletResponse servletResponse,
      FilterChain filterChain
  ) throws IOException, ServletException
  {
    HttpServletResponse httpResp = (HttpServletResponse) servletResponse;

    // An authenticator hint supplied by the client can be used to avoid attempting an authenticator that an informed client knows will fail
    // For instance, if we are chaining metadata and LDAP authenticators and the client is aware that it only used LDAP authenticators,
    // it makes no sense for the server to try the metadata authenticator needlessly.
    String authenticatorHint = ((HttpServletRequest) servletRequest).getHeader("X-DRUID-AUTHENTICATOR-HINT");
    if (null != authenticatorHint && !"druid-ldap-security".equals(authenticatorHint)) {
      log.debug(
          "Client supplied authenticator hint [%s] does not match authenticator name [%s]. Moving on to next filter",
          authenticatorHint,
          "druid-ldap-security"
      );
      filterChain.doFilter(servletRequest, servletResponse);
      return;
    }

    String encodedUserSecret = LdapAuthUtils.getEncodedUserSecretFromHttpReq((HttpServletRequest) servletRequest);
    if (encodedUserSecret == null) {
      log.info("----------- Request didn't have valid HTTP Basic Authorization header, move on to the next filter");
      // Request didn't have HTTP Basic auth credentials, move on to the next filter
      filterChain.doFilter(servletRequest, servletResponse);
      return;
    }

    String decodedUserSecret = LdapAuthUtils.decodeUserSecret(encodedUserSecret);
    if (decodedUserSecret == null) {
      log.info("----------- Request HTTP Basic Authorization header secret could not be decoded");
      // we recognized a Basic auth header, but could not decode the user secret
      httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }

    String[] splits = decodedUserSecret.split(":");
    if (splits.length != 2) {
      log.info("----------- Request HTTP Basic Authorization header secret is invalid format");
      httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }

    String user = splits[0];
    String password = splits[1];

    boolean validated = validateCredentials(user, password);
    emitter.emit(
        new AuditEvent.Builder()
            .setDimension("authenticator", "druid-ldap-security")
            .setDimension("result", validated ? "PASS" : "FAIL")
            .build(user, AuditEvent.EventType.AUTHENTICATION_RESULT)
    );
    if (validated) {
      log.debug("----------- Request with valid user credentials submitted");
      AuthenticationResult authenticationResult = new AuthenticationResult(user, authorizerName, null, null);
      servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
      filterChain.doFilter(servletRequest, servletResponse);
    } else {
      log.info("----------- Request with invalid user credentials submitted");
      if (skipOnFailure) {
        log.info("Skipping failed authenticator %s ", "druid-ldap-security");
        filterChain.doFilter(servletRequest, servletResponse);
      } else {
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
      }
    }
  }

  public boolean validateCredentials(String username, String password)
  {
    Set<String> credentials = clientCredentials.get();
    log.debug("----------- Verifying credentials. username: %s", username);

    if (credentials.contains(org.apache.druid.java.util.common.StringUtils.format("%s:%s", username, password))) {
      log.debug("----------- Credentials verified. username: %s", username);
      return true;
    } else {
      log.info("----------- Credentials not be verified. username: %s", username);
      return false;
    }
  }

  @Override
  public void destroy()
  {
    thread.shutdown();
  }

  private void loadClientCredentials(File file, AtomicReference<Set<String>> clientCredentials) throws IOException
  {
    List<String> lines = Files.readLines(file, StandardCharsets.UTF_8);
    Set<String> creds = new HashSet<>();
    int lineNum = 0;
    for (String line : lines) {
      lineNum++;
      String[] parts = line.split(":");
      if (parts.length < 2) {
        log.warn("Malformed credential found on line %d", lineNum);
        continue;
      }
      creds.add(org.apache.druid.java.util.common.StringUtils.format("%s:%s", parts[0], parts[1]));
    }
    clientCredentials.set(Collections.unmodifiableSet(creds));
  }

  private class ClientCredentialsThread extends Thread
  {

    private final String lookupFile;
    private final int pollFrequency;
    private final AtomicReference<Set<String>> clientCredentials;

    private long lastModified;

    private volatile boolean shutdown = false;

    public ClientCredentialsThread(String lookupFile, int pollFrequency, AtomicReference<Set<String>> clientCredentials)
    {
      this.lookupFile = lookupFile;
      this.pollFrequency = pollFrequency;
      this.clientCredentials = clientCredentials;
      this.lastModified = new File(this.lookupFile).lastModified();
    }

    @Override
    public void run()
    {
      File file = new File(this.lookupFile);
      while (!shutdown) {
        try {
          if (file.lastModified() != lastModified) {
            log.info("----------- Reloading client credentials file: %s", this.lookupFile);
            loadClientCredentials(file, this.clientCredentials);
            this.lastModified = file.lastModified();
          }
          TimeUnit.SECONDS.sleep(pollFrequency);
        }
        catch (IOException ioe) {
          log.error(ioe, "----------- Error loading client credentials file: %s", lookupFile);
        }
        catch (InterruptedException ie) {
          log.debug("----------- Interrupted client credentials thread");
        }
      }
    }

    public void shutdown()
    {
      shutdown = true;
      interrupt();
    }
  }

}
