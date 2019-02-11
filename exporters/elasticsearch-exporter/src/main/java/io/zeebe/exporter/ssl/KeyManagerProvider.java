package io.zeebe.exporter.ssl;

import javax.net.ssl.KeyManager;

public interface KeyManagerProvider {

  KeyManager[] provide();
}
