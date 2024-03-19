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
package io.camunda.zeebe.client.impl.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.CredentialsProvider;
import io.camunda.zeebe.client.ZeebeClientConfiguration;
import io.camunda.zeebe.client.impl.NoopCredentialsProvider;
import io.camunda.zeebe.client.impl.util.VersionUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.config.RequestConfig.Builder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.HttpClientHostnameVerifier;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.config.CharCodingConfig;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

public class HttpClientFactory {

  private static final String REST_API_PATH = "/v1";
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private final ZeebeClientConfiguration config;

  public HttpClientFactory(final ZeebeClientConfiguration config) {
    this.config = config;
  }

  public HttpClient createClient() {
    final RequestConfig defaultRequestConfig = defaultClientRequestConfigBuilder().build();
    final CloseableHttpAsyncClient client =
        defaultClientBuilder().setDefaultRequestConfig(defaultRequestConfig).build();
    final URI gatewayAddress = buildGatewayAddress();
    final CredentialsProvider credentialsProvider =
        config.getCredentialsProvider() != null
            ? config.getCredentialsProvider()
            : new NoopCredentialsProvider();

    return new HttpClient(
        client,
        JSON_MAPPER,
        gatewayAddress,
        defaultRequestConfig,
        config.getMaxMessageSize(),
        TimeValue.ofSeconds(15),
        credentialsProvider);
  }

  private URI buildGatewayAddress() {
    try {
      final URIBuilder builder = new URIBuilder(config.getRestAddress()).setPath(REST_API_PATH);
      builder.setScheme(config.isPlaintextConnectionEnabled() ? "http" : "https");

      return builder.build();
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private HttpAsyncClientBuilder defaultClientBuilder() {
    final Header acceptHeader =
        new BasicHeader(
            HttpHeaders.ACCEPT,
            String.join(
                ", ",
                ContentType.APPLICATION_JSON.getMimeType(),
                ContentType.APPLICATION_PROBLEM_JSON.getMimeType()));

    final HttpClientHostnameVerifier hostnameVerifier =
        new HostnameVerifier(config.getOverrideAuthority());
    final TlsStrategy tlsStrategy =
        ClientTlsStrategyBuilder.create()
            .setSslContext(createSslContext())
            .setHostnameVerifier(hostnameVerifier)
            .build();
    final PoolingAsyncClientConnectionManager connectionManager =
        PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build();

    return HttpAsyncClients.custom()
        .setConnectionManager(connectionManager)
        .setDefaultHeaders(Collections.singletonList(acceptHeader))
        .setUserAgent("zeebe-client-java/" + VersionUtil.getVersion())
        .evictExpiredConnections()
        .setCharCodingConfig(CharCodingConfig.custom().setCharset(StandardCharsets.UTF_8).build())
        .evictIdleConnections(TimeValue.ofSeconds(30))
        .useSystemProperties(); // allow users to customize via system properties
  }

  private Builder defaultClientRequestConfigBuilder() {
    return RequestConfig.custom()
        .setResponseTimeout(Timeout.of(config.getDefaultRequestTimeout()))
        // TODO: determine if the existing (gRPC) property makes sense for the HTTP client
        .setConnectionKeepAlive(TimeValue.of(config.getKeepAlive()))
        // hard cancellation may cause other requests to fail as it will kill the connection; can be
        // enabled when using HTTP/2
        .setHardCancellationEnabled(false);
  }

  private SSLContext createSslContext() {
    if (config.isPlaintextConnectionEnabled() || config.getCaCertificatePath() == null) {
      return SSLContexts.createDefault();
    }

    final KeyStore keyStore = createKeyStore();
    try {
      final TrustManagerFactory factory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init(keyStore);

      final SSLContext context = SSLContexts.custom().build();
      context.init(null, factory.getTrustManagers(), null);
      return context;
    } catch (final NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
      throw new RuntimeException(e);
    }
  }

  private KeyStore createKeyStore() {
    final File pemChain = new File(config.getCaCertificatePath());

    try (final FileInputStream certificateStream = new FileInputStream(pemChain)) {
      final KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
      store.load(null);

      final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
      int i = 1;
      while (certificateStream.available() > 0) {
        final Certificate certificate = certificateFactory.generateCertificate(certificateStream);
        final String alias = Integer.toString(i);
        store.setCertificateEntry(alias, certificate);
        i++;
      }

      return store;
    } catch (final CertificateException
        | IOException
        | KeyStoreException
        | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static final class HostnameVerifier implements HttpClientHostnameVerifier {
    private final HttpClientHostnameVerifier delegate = new DefaultHostnameVerifier();
    private final String overriddenAuthority;

    private HostnameVerifier(final String overriddenAuthority) {
      this.overriddenAuthority = overriddenAuthority;
    }

    @Override
    public void verify(final String host, final X509Certificate cert) throws SSLException {
      final String hostname = host.equals("0.0.0.0") ? "localhost" : host;
      if (overriddenAuthority != null) {
        delegate.verify(overriddenAuthority, cert);
      } else {

        delegate.verify(hostname, cert);
      }
    }

    @Override
    public boolean verify(final String hostname, final SSLSession session) {
      final String host = hostname.equals("0.0.0.0") ? "localhost" : hostname;
      return delegate.verify(overriddenAuthority == null ? host : overriddenAuthority, session);
    }
  }
}
