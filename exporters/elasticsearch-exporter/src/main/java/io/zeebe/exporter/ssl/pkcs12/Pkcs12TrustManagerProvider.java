package io.zeebe.exporter.ssl.pkcs12;

import io.zeebe.exporter.ElasticsearchExporterConfiguration.SslConfiguration;
import io.zeebe.exporter.ElasticsearchExporterException;
import io.zeebe.exporter.ssl.TrustManagerProvider;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class Pkcs12TrustManagerProvider implements TrustManagerProvider {

  private final Pkcs12KeyStoreProvider keyStoreProvider;

  public Pkcs12TrustManagerProvider(
    SslConfiguration configuration) {
    this(new Pkcs12KeyStoreProvider(configuration.trustStore, configuration.trustStorePassword));
  }

  public Pkcs12TrustManagerProvider(
    Pkcs12KeyStoreProvider keyStoreProvider) {
    this.keyStoreProvider = keyStoreProvider;
  }

  @Override
  public TrustManager[] provide() {
    final TrustManagerFactory factory = getTrustManagerFactory();
    try {
      factory.init(keyStoreProvider.provide());
    } catch (KeyStoreException e) {
      throw new ElasticsearchExporterException("Failed to initial trust manager factory", e);
    }

    return factory.getTrustManagers();
  }

  private TrustManagerFactory getTrustManagerFactory() {
    try {
      return TrustManagerFactory
        .getInstance(TrustManagerFactory.getDefaultAlgorithm());
    } catch (NoSuchAlgorithmException e) {
      throw new ElasticsearchExporterException(
        "Failed to create new instance of trust manager factory", e);
    }
  }
}
