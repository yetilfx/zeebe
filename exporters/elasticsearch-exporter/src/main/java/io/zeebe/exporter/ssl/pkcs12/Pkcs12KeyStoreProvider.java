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

import io.zeebe.exporter.ElasticsearchExporterException;
import io.zeebe.exporter.ssl.KeyStoreProvider;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;

public class Pkcs12KeyStoreProvider implements KeyStoreProvider {
  private static final String DEFAULT_STORE_TYPE = "PKCS12";
  private static final String DEFAULT_PROVIDER = "SunJSSE";

  private final String path;
  private final char[] storePassword;

  public Pkcs12KeyStoreProvider(String path, String storePassword) {
    this.path = path;
    this.storePassword = toPasswordCharArray(storePassword);
  }

  @Override
  public KeyStore provide() {
    final InputStream source;

    try {
      source = openFileOrResource(path);
    } catch (IOException e) {
      throw new ElasticsearchExporterException("Failed to read key store file", e);
    }

    if (source == null) {
      throw new ElasticsearchExporterException("No key store found on disk or in the classpath");
    }

    final KeyStore keyStore = getKeyStore();
    try {
      keyStore.load(source, getStorePassword());
    } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
      throw new ElasticsearchExporterException("Failed to load key store source", e);
    }

    return keyStore;
  }

  @Override
  public char[] getStorePassword() {
    return storePassword;
  }

  private char[] toPasswordCharArray(String password) {
    if (password == null) {
      return new char[0];
    } else {
      return password.toCharArray();
    }
  }

  private KeyStore getKeyStore() {
    try {
      try {
        return KeyStore.getInstance(DEFAULT_STORE_TYPE, DEFAULT_PROVIDER);
      } catch (NoSuchProviderException e) {
        return KeyStore.getInstance(DEFAULT_STORE_TYPE);
      }
    } catch (KeyStoreException e) {
      throw new ElasticsearchExporterException("Failed to create new PKCS12 key store", e);
    }
  }

  private InputStream openFileOrResource(String path) throws IOException {
    final InputStream resource = getClass().getClassLoader().getResourceAsStream(path);

    if (resource == null) {
      final File file = new File(path);

      if (file.exists()) {
        return Files.newInputStream(file.toPath());
      }
    }

    return resource;
  }
}
