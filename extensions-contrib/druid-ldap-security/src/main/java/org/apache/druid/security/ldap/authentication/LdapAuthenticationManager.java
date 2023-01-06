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
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.ldap.LdapAuthCommonConfig;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class LdapAuthenticationManager implements AuthenticationManager
{
  private static final Logger log = new Logger(LdapAuthenticationManager.class);

  private LdapAuthCommonConfig ldapAuthCommonConfig;
  private final Set<String> credentials;

  @Inject
  public LdapAuthenticationManager(
      LdapAuthCommonConfig ldapAuthCommonConfig
  ) throws IOException
  {
    this.ldapAuthCommonConfig = ldapAuthCommonConfig;
    File lookupFile = new File(this.ldapAuthCommonConfig.getLookupFile());
    credentials = Collections.unmodifiableSet(new HashSet<>(Files.readLines(lookupFile, StandardCharsets.UTF_8)));
    log.info("----------- LdapAuthenticationManager initialized");
  }

  @Override
  public boolean validateCredentials(String username, String password)
  {
    log.info("----------- Verifying credentials");

    if (credentials.isEmpty()) {
      log.info("----------- Credentials set empty");
      return true;
    }

    if (credentials.contains(String.format(Locale.ENGLISH, "%s:%s", username, password))) {
      log.info("----------- Credentials verified");
      return true;
    } else {
      log.info("----------- Invalid credentials submitted");
      return false;
    }
  }
}
