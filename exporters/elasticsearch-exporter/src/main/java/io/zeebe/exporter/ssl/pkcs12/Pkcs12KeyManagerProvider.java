package io.zeebe.exporter.ssl.pkcs12;

import io.zeebe.exporter.ElasticsearchExporterConfiguration.SslConfiguration;
import io.zeebe.exporter.ElasticsearchExporterException;
import io.zeebe.exporter.ssl.KeyManagerProvider;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;

public class Pkcs12KeyManagerProvider implements KeyManagerProvider {

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
    final char[] password;

    try {
      final KeyStore keyStore = keyStoreProvider.provide();
      keyManagerFactory.init(keyStore, keyStoreProvider.getPassword());
    } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
      throw new ElasticsearchExporterException("Failed to obtain key managers", e);
    }

    return keyManagerFactory.getKeyManagers();
  }

  private KeyManagerFactory getKeyManagerFactory() {
    try {
      return KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    } catch (NoSuchAlgorithmException e) {
      throw new ElasticsearchExporterException(
        "Failed to get a new instance of a KeyManagerFactory", e);
    }
  }
}
