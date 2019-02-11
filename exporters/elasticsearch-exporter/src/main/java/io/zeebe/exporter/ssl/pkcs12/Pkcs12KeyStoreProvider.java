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
import java.security.cert.CertificateException;

public class Pkcs12KeyStoreProvider implements KeyStoreProvider {

  private final String path;
  private final String password;

  public Pkcs12KeyStoreProvider(String path, String password) {
    this.path = path;
    this.password = password;
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

    final KeyStore keyStore;
    try {
      keyStore = KeyStore.getInstance("pkcs12");
    } catch (KeyStoreException e) {
      throw new ElasticsearchExporterException("Failed to create new PKCS12 key store", e);
    }

    try {
      keyStore.load(source, getPassword());
    } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
      throw new ElasticsearchExporterException("Failed to load key store source", e);
    }

    return keyStore;
  }

  @Override
  public char[] getPassword() {
    if (password == null || password.isEmpty()) {
      return null;
    } else {
      return password.toCharArray();
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
