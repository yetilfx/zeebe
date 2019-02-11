package io.zeebe.exporter.ssl;

import io.zeebe.exporter.ElasticsearchExporterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class DefaultTrustManagerProvider implements TrustManagerProvider {

  @Override
  public TrustManager[] provide() {
    final TrustManagerFactory factory;
    try {
      factory = TrustManagerFactory
        .getInstance(TrustManagerFactory.getDefaultAlgorithm());
    } catch (NoSuchAlgorithmException e) {
      throw new ElasticsearchExporterException("Failed to create new trust manager factory", e);
    }

    try {
      factory.init((KeyStore) null);
    } catch (KeyStoreException e) {
      throw new ElasticsearchExporterException(
        "Failed to initial default trust manager with empty key store", e);
    }

    return factory.getTrustManagers();
  }
}
