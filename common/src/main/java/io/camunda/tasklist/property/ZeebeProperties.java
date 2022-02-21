/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */
package io.camunda.tasklist.property;

public class ZeebeProperties {

  private String gatewayAddress = "localhost:26500";
  private boolean isSecure = false;
  private String certificatePath = null;

  public boolean isSecure() {
    return isSecure;
  }

  public ZeebeProperties setSecure(final boolean secure) {
    isSecure = secure;
    return this;
  }

  public String getCertificatePath() {
    return certificatePath;
  }

  public ZeebeProperties setCertificatePath(final String caCertificatePath) {
    this.certificatePath = caCertificatePath;
    return this;
  }

  @Deprecated
  public String getBrokerContactPoint() {
    return gatewayAddress;
  }

  @Deprecated
  public void setBrokerContactPoint(String brokerContactPoint) {
    this.gatewayAddress = brokerContactPoint;
  }

  public String getGatewayAddress() {
    return gatewayAddress;
  }

  public ZeebeProperties setGatewayAddress(final String gatewayAddress) {
    this.gatewayAddress = gatewayAddress;
    return this;
  }
}
