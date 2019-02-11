package io.zeebe.exporter.ssl;

import javax.net.ssl.TrustManager;

public interface TrustManagerProvider {

  TrustManager[] provide();
}
