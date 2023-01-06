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

package org.apache.druid.security.ldap;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.security.ldap.authentication.LdapAuthenticator;
import org.apache.druid.security.ldap.authentication.LdapEscalator;
import org.apache.druid.security.ldap.authorization.LdapAuthorizer;

import java.util.List;

public class LdapSecurityDruidModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.auth.ldap.common", LdapAuthCommonConfig.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("LdapDruidSecurity").registerSubtypes(
            LdapAuthenticator.class,
            LdapEscalator.class,
            LdapAuthorizer.class
        )
    );
  }

  private static boolean isCoordinator(Injector injector)
  {
    final String serviceName;
    try {
      serviceName = injector.getInstance(Key.get(String.class, Names.named("serviceName")));
    }
    catch (Exception e) {
      return false;
    }

    return "druid/coordinator".equals(serviceName);
  }
}
