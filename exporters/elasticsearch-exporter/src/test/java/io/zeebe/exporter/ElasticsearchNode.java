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

import com.github.dockerjava.api.model.Ulimit;
import java.io.IOException;
import java.time.Duration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.SelinuxContext;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

/** Configurable Elasticsearch container */
public class ElasticsearchNode extends GenericContainer<ElasticsearchNode> {

  private static final int DEFAULT_HTTP_PORT = 9200;
  private static final int DEFAULT_TCP_PORT = 9300;
  private static final String DEFAULT_TAG = "6.6.0";
  private static final String DEFAULT_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";

  private boolean isSslEnabled;

  private boolean isAuthEnabled;
  private final String username = "elastic";
  private String password;

  private RestHighLevelClient client;

  public ElasticsearchNode() {
    this(DEFAULT_IMAGE, DEFAULT_TAG);
  }

  public ElasticsearchNode(String image, String tag) {
    super(image + ":" + tag);

    withNetworkAliases("elasticsearch-" + Base58.randomString(6));
    withEnv("discovery.type", "single-node");
    addExposedPorts(DEFAULT_HTTP_PORT, DEFAULT_TCP_PORT);

    // set ulimits according to
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#next-getting-started-tls-docker
    withUlimit("memlock", -1, -1)
        .withUlimit("nofile", 65536, 65536)
        .withEnv("bootstrap.memory_lock", "true");
  }

  @Override
  public void start() {
    setupHttpStrategy();
    super.start();
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

  public ElasticsearchNode withUlimit(String name, int soft, int hard) {
    withCreateContainerCmdModifier(cmd -> cmd.withUlimits(new Ulimit(name, soft, hard)));
    return this;
  }

  public ElasticsearchNode withXpack() {
    withEnv("xpack.license.self_generated.type", "trial");

    return this;
  }

  public ElasticsearchNode withPassword(String password) {
    withXpack().withEnv("xpack.security.enabled", "true").withEnv("ELASTIC_PASSWORD", password);
    isAuthEnabled = true;
    this.password = password;

    return this;
  }

  public ElasticsearchNode withJavaOptions(String... options) {
    withEnv("ES_JAVA_OPTS", String.join(", ", options));
    return this;
  }

  /**
   * Sets the server keystore, that is, the store containing the server certificate and certificate
   * authorities. Also implicitly trusts it for as long as the JVM is alive.
   */
  public ElasticsearchNode withKeyStore(String keyStore) {
    final String containerPath = "/usr/share/elasticsearch/config/certs/keyStore.p12";
    withXpack()
        .withEnv("xpack.security.http.ssl.enabled", "true")
        .withEnv("xpack.security.http.ssl.keystore.path", containerPath)
        .withClasspathResourceMapping(
            keyStore, containerPath, BindMode.READ_ONLY, SelinuxContext.SHARED);

    isSslEnabled = true;

    return this;
  }

  /**
   * Sets the server trust store, that is, the store containing the client certificate authority.
   */
  public ElasticsearchNode withTrustStore(
      String trustStore, String storePassword, String keyPassword) {
    final String containerPath = "/usr/share/elasticsearch/config/certs/trustStore.p12";
    withXpack()
        .withEnv("xpack.security.http.ssl.enabled", "true")
        .withEnv("xpack.security.http.ssl.truststore.path", containerPath)
        .withClasspathResourceMapping(
            trustStore, containerPath, BindMode.READ_ONLY, SelinuxContext.SHARED);

    if (storePassword != null) {
      withEnv("xpack.security.http.ssl.truststore.password", storePassword);
    }

    if (keyPassword != null) {
      withEnv("xpack.security.http.ssl.truststore.key_password", keyPassword);
    }

    isSslEnabled = true;

    return this;
  }

  public HttpHost getRestHttpHost() {
    assert isCreated() : "cannot get http host until the container is started";

    final String scheme = isSslEnabled ? "https" : "http";
    return new HttpHost(getContainerIpAddress(), getMappedPort(DEFAULT_HTTP_PORT), scheme);
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

  private void setupHttpStrategy() {
    final HttpWaitStrategy waitStrategy = new HttpWaitStrategy();

    waitStrategy
        .forPort(DEFAULT_HTTP_PORT)
        .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
        .withStartupTimeout(Duration.ofMinutes(2));

    if (isAuthEnabled) {
      waitStrategy.withBasicCredentials(username, password);
    }

    if (isSslEnabled) {
      waitStrategy.usingTls();
    }

    setWaitStrategy(waitStrategy);
  }
}
