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
package io.zeebe.exporter;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import io.zeebe.exporter.ssl.DefaultKeyManagerProvider;
import io.zeebe.exporter.ssl.DefaultTrustManagerProvider;
import io.zeebe.exporter.ssl.KeyManagerProvider;
import io.zeebe.exporter.ssl.SSLContextFactory;
import io.zeebe.exporter.ssl.TrustManagerProvider;
import io.zeebe.exporter.ssl.pkcs12.Pkcs12KeyManagerProvider;
import io.zeebe.exporter.ssl.pkcs12.Pkcs12KeyStoreProvider;
import io.zeebe.exporter.ssl.pkcs12.Pkcs12TrustManagerProvider;
import java.io.IOException;
import java.time.Duration;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

/**
 * Wraps around {@link org.testcontainers.elasticsearch.ElasticsearchContainer} to provide some
 * convenience methods.
 */
public class ElasticsearchNode extends ElasticsearchContainer {

  private static final int DEFAULT_PORT = 9200;
  private static final String DEFAULT_TAG = "6.6.0";
  private static final String DEFAULT_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";

  private boolean isAuthEnabled;
  private final String username = "elastic";
  private String password;

  private boolean isSslEnabled;
  private KeyManagerProvider keyManagerProvider = new DefaultKeyManagerProvider();
  private TrustManagerProvider trustManagerProvider = new DefaultTrustManagerProvider();

  private RestHighLevelClient client;

  private HttpWaitStrategy waitStrategy;

  public ElasticsearchNode() {
    this(DEFAULT_IMAGE, DEFAULT_TAG);
  }

  public ElasticsearchNode(String image, String tag) {
    super(image + ":" + tag);
    this.waitStrategy = new HttpWaitStrategy();
    setWaitStrategy(waitStrategy);

    waitStrategy
        .forPort(DEFAULT_PORT)
        .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
        .withStartupTimeout(Duration.ofMinutes(2));
  }

  @Override
  public void stop() {
    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        logger().error("Failed to close Elasticsearch REST client", e);
      }

      client = null;
    }

    super.stop();
  }

  public ElasticsearchNode enableXpack() {
    withEnv("xpack.license.self_generated.type", "trial");

    return this;
  }

  public ElasticsearchContainer withPassword(String password) {
    enableXpack().withEnv("xpack.security.enabled", "true").withEnv("ELASTIC_PASSWORD", password);
    isAuthEnabled = true;
    waitStrategy.withBasicCredentials(username, password);

    return this;
  }

  /**
   * Sets the server keystore, that is, the store containing the server certificate and certificate
   * authorities. Also implicitly trusts it for as long as the JVM is alive.
   */
  public ElasticsearchNode withKeyStore(String keyStore, String password) {
    final String containerPath = "/usr/share/elasticsearch/config/certs/keyStore.p12";
    enableXpack()
        .withEnv("xpack.security.http.ssl.enabled", "true")
        .withEnv("xpack.security.http.ssl.keystore.path", containerPath)
        .withClasspathResourceMapping(
          keyStore, containerPath, BindMode.READ_ONLY, SelinuxContext.SHARED);

    if (password != null) {
      withEnv("xpack.security.http.ssl.keystore.password", password);
    }

    isSslEnabled = true;
    waitStrategy.usingTls();
    trustManagerProvider = new Pkcs12TrustManagerProvider(
      new Pkcs12KeyStoreProvider(keyStore, password));
    configureHttpWaitStrategyForSsl();

    return this;
  }

  /**
   * Sets the server trust store, that is, the store containing the client certificate authority.
   */
  public ElasticsearchNode withTrustStore(String trustStore, String password) {
    final String containerPath = "/usr/share/elasticsearch/config/certs/trustStore.p12";
    enableXpack()
      .withEnv("xpack.security.http.ssl.enabled", "true")
      .withEnv("xpack.security.http.ssl.truststore.path", containerPath)
      .withClasspathResourceMapping(
        trustStore, containerPath, BindMode.READ_ONLY, SelinuxContext.SHARED);

    if (password != null) {
      withEnv("xpack.security.http.ssl.truststore.password", password);
    }

    withEnv("ES_JAVA_OPTS", "-Djavax.net.debug=ssl");

    isSslEnabled = true;
    waitStrategy.usingTls();
    keyManagerProvider = new Pkcs12KeyManagerProvider(
      new Pkcs12KeyStoreProvider(trustStore, password));
    configureHttpWaitStrategyForSsl();

    return this;
  }

  public HttpHost getRestHttpHost() {
    assert isCreated() : "cannot get http host until the container is started";

    final String scheme = isSslEnabled ? "https" : "http";
    return new HttpHost(getContainerIpAddress(), getMappedPort(DEFAULT_PORT), scheme);
  }

  public RestHighLevelClient getClient() {
    if (client == null) {
      client = new RestHighLevelClient(newClient());
    }

    return client;
  }

  public RestClientBuilder newClient() {
    return newClient(null);
  }

  public RestClientBuilder newClient(RestClientBuilder.HttpClientConfigCallback configurator) {
    assert isCreated() : "cannot create a client until the container is started";

    final RestClientBuilder builder = RestClient.builder(getRestHttpHost());
    builder.setHttpClientConfigCallback(
        config -> {
          if (isAuthEnabled) {
            setupClientAuthentication(config);
          }

          if (configurator != null) {
            config = configurator.customizeHttpClient(config);
          }

          return config;
        });

    return builder;
  }

  private void setupClientAuthentication(HttpAsyncClientBuilder config) {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(username, password));
    config.setDefaultCredentialsProvider(credentialsProvider);
  }

  private void configureHttpWaitStrategyForSsl() {
    final SSLContext sslContext = new SSLContextFactory("TLS")
      .newContext(keyManagerProvider, trustManagerProvider);

    // required for the HttpWaitStrategy
    HttpsURLConnection.setDefaultHostnameVerifier(NoopHostnameVerifier.INSTANCE);
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
  }
}
