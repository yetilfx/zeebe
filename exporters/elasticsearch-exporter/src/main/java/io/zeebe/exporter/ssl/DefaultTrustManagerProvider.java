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
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class DefaultTrustManagerProvider implements TrustManagerProvider {
  private static final String DEFAULT_PROVIDER = "SunJSSE";
  private static final String DEFAULT_ALGORITHM = "PKIX";

  @Override
  public TrustManager[] provide() {
    final TrustManagerFactory factory = getTrustManagerFactory();

    try {
      factory.init((KeyStore) null);
    } catch (KeyStoreException e) {
      throw new ElasticsearchExporterException(
          "Failed to initial default trust manager with empty key store", e);
    }

    return factory.getTrustManagers();
  }

  private TrustManagerFactory getTrustManagerFactory() {
    try {
      try {
        return TrustManagerFactory.getInstance(DEFAULT_ALGORITHM, DEFAULT_PROVIDER);
      } catch (NoSuchProviderException e) {
        return TrustManagerFactory.getInstance(DEFAULT_ALGORITHM);
      }
    } catch (NoSuchAlgorithmException e) {
      throw new ElasticsearchExporterException(
          "Failed to create new instance of trust manager factory", e);
    }
  }
}
