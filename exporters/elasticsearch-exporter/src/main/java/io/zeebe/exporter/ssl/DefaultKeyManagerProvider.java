package io.zeebe.exporter.ssl;

import javax.net.ssl.KeyManager;

public class DefaultKeyManagerProvider implements KeyManagerProvider {

  @Override
  public KeyManager[] provide() {
    return new KeyManager[0];
  }
}
