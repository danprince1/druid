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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.servlet.http.HttpServletRequest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

public class LdapAuthUtils
{

  private static final Logger log = new Logger(LdapAuthUtils.class);
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  // PBKDF2WithHmacSHA512 is chosen since it has built-in support in Java8.
  // Argon2 (https://github.com/p-h-c/phc-winner-argon2) is newer but the only presently
  // available Java binding is LGPLv3 licensed.
  // Key length is 512-bit to match the PBKDF2WithHmacSHA512 algorithm.
  // 256-bit salt should be more than sufficient for uniqueness, expected user count is on the order of thousands.
  public static final int SALT_LENGTH = 32;
  public static final int DEFAULT_KEY_ITERATIONS = 10000;
  public static final int KEY_LENGTH = 512;
  public static final String ALGORITHM = "PBKDF2WithHmacSHA512";


  public static String getEncodedCredentials(final String unencodedCreds)
  {
    return Base64.getEncoder().encodeToString(StringUtils.toUtf8(unencodedCreds));
  }

  public static byte[] hashPassword(final char[] password, final byte[] salt, final int iterations)
  {
    try {
      SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM);
      SecretKey key = keyFactory.generateSecret(
          new PBEKeySpec(
              password,
              salt,
              iterations,
              KEY_LENGTH
          )
      );
      return key.getEncoded();
    }
    catch (InvalidKeySpecException ikse) {
      log.error("invalid keyspec");
      throw new RuntimeException("invalid keyspec", ikse);
    }
    catch (NoSuchAlgorithmException nsae) {
      log.error("%s not supported on this system.", ALGORITHM);
      throw new RuntimeException(StringUtils.format("%s not supported on this system.", ALGORITHM), nsae);
    }
  }

  public static byte[] generateSalt()
  {
    byte salt[] = new byte[SALT_LENGTH];
    SECURE_RANDOM.nextBytes(salt);
    return salt;
  }

  @Nullable
  public static String getEncodedUserSecretFromHttpReq(HttpServletRequest httpReq)
  {
    String authHeader = httpReq.getHeader("Authorization");

    if (authHeader == null) {
      return null;
    }

    if (authHeader.length() < 7) {
      return null;
    }

    if (!authHeader.substring(0, 6).equals("Basic ")) {
      return null;
    }

    return authHeader.substring(6);
  }

  @Nullable
  public static String decodeUserSecret(String encodedUserSecret)
  {
    try {
      return StringUtils.fromUtf8(Base64.getDecoder().decode(encodedUserSecret));
    }
    catch (IllegalArgumentException iae) {
      return null;
    }
  }

}
