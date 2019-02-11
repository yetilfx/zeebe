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

import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.http.conn.ssl.TrustStrategy;

public class SSLContextFactory {

  /**
   * Currently trusts all certificates for the given protocol; accepts two parameters to keep it
   * flexible in the future, as we should find a way to accept only the given one and not all
   * certificates.
   */
  public static SSLContext newContext(String protocol, KeyStore keyStore, TrustStrategy strategy)
      throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
    final SSLContext context = SSLContext.getInstance(protocol);
    final TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(keyStore);
    final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

    if (trustManagers != null) {
      for (int i = 0; i < trustManagers.length; i++) {
        final TrustManager trustManager = trustManagers[i];
        if (trustManager instanceof X509TrustManager) {
          trustManagers[i] = new TrustManagerDelegate((X509TrustManager) trustManager, strategy);
        }
      }
    }

    context.init(null, trustManagers, new SecureRandom());
    return context;
  }
}
