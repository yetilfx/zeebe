package io.zeebe.exporter.ssl;

import io.zeebe.exporter.ElasticsearchExporterException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.net.ssl.SSLContext;

public class SSLContextFactory {

  private final String protocol;

  public SSLContextFactory() {
    this("TLS");
  }

  public SSLContextFactory(String protocol) {
    this.protocol = protocol;
  }

  public SSLContext newContext(KeyManagerProvider keyManagerProvider,
    TrustManagerProvider trustManagerProvider) {
    return newContext(keyManagerProvider, trustManagerProvider, new SecureRandom());
  }

  public SSLContext newContext(KeyManagerProvider keyManagerProvider,
    TrustManagerProvider trustManagerProvider, SecureRandom secureRandom) {
    final SSLContext context;

    try {
      context = SSLContext.getInstance(protocol);
    } catch (NoSuchAlgorithmException e) {
      throw new ElasticsearchExporterException("Failed to create new SSL context", e);
    }

    try {
      context.init(keyManagerProvider.provide(), trustManagerProvider.provide(), secureRandom);
    } catch (KeyManagementException e) {
      throw new ElasticsearchExporterException("Failed to initialize new SSL context", e);
    }

    return context;
  }
}
