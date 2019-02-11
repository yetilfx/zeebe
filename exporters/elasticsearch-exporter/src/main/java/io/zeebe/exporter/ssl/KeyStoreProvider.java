package io.zeebe.exporter.ssl;

import java.security.KeyStore;

public interface KeyStoreProvider {

  KeyStore provide();

  char[] getPassword();
}
