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
package io.zeebe.exporter.ssl.pkcs12;

import io.zeebe.exporter.ElasticsearchExporterConfiguration.SslConfiguration;
import io.zeebe.exporter.ElasticsearchExporterException;
import io.zeebe.exporter.ssl.KeyManagerProvider;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;

public class Pkcs12KeyManagerProvider implements KeyManagerProvider {
  private static final String DEFAULT_PROVIDER = "SunJSSE";
  private static final String DEFAULT_ALGORITHM = "PKIX";

  private final Pkcs12KeyStoreProvider keyStoreProvider;

  public Pkcs12KeyManagerProvider(SslConfiguration configuration) {
    this(new Pkcs12KeyStoreProvider(configuration.keyStore, configuration.keyStorePassword));
  }

  public Pkcs12KeyManagerProvider(Pkcs12KeyStoreProvider keyStoreProvider) {
    this.keyStoreProvider = keyStoreProvider;
  }

  @Override
  public KeyManager[] provide() {
    final KeyManagerFactory keyManagerFactory = getKeyManagerFactory();

    try {
      final KeyStore keyStore = keyStoreProvider.provide();
      keyManagerFactory.init(keyStore, keyStoreProvider.getStorePassword());
    } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
      throw new ElasticsearchExporterException("Failed to obtain key managers", e);
    }

    return keyManagerFactory.getKeyManagers();
  }

  private KeyManagerFactory getKeyManagerFactory() {
    try {
      try {
        return KeyManagerFactory.getInstance(DEFAULT_ALGORITHM, DEFAULT_PROVIDER);
      } catch (NoSuchProviderException e) {
        return KeyManagerFactory.getInstance(DEFAULT_ALGORITHM);
      }
    } catch (NoSuchAlgorithmException e) {
      throw new ElasticsearchExporterException(
          "Failed to get a new instance of a KeyManagerFactory", e);
    }
  }
}
