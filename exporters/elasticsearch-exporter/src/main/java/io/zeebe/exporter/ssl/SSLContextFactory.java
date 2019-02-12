/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporter.ssl;

import io.zeebe.exporter.ElasticsearchExporterException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import javax.net.ssl.SSLContext;

public class SSLContextFactory {
  private static final String DEFAULT_PROTOCOL = "TLSv1.2";
  private static final String DEFAULT_PROVIDER = "SunJSSE";

  private final String protocol;

  public SSLContextFactory() {
    this(DEFAULT_PROTOCOL);
  }

  public SSLContextFactory(String protocol) {
    this.protocol = protocol;
  }

  public SSLContext newContext(
      KeyManagerProvider keyManagerProvider, TrustManagerProvider trustManagerProvider) {
    return newContext(keyManagerProvider, trustManagerProvider, new SecureRandom());
  }

  public SSLContext newContext(
      KeyManagerProvider keyManagerProvider,
      TrustManagerProvider trustManagerProvider,
      SecureRandom secureRandom) {
    SSLContext context;

    try {
      try {
        context = SSLContext.getInstance(protocol, DEFAULT_PROVIDER);
      } catch (NoSuchProviderException e) {
        context = SSLContext.getInstance(protocol);
      }
    } catch (NoSuchAlgorithmException e) {
      throw new ElasticsearchExporterException("Failed to create new SSL context", e);
    }

    try {
      context.init(keyManagerProvider.provide(), trustManagerProvider.provide(), secureRandom);
    } catch (KeyManagementException e) {
      throw new ElasticsearchExporterException("Failed to initialize new SSL context", e);
    }

    return context;
  }
}
