/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.authorization;

import io.camunda.zeebe.db.DbValue;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.msgpack.property.ArrayProperty;
import io.camunda.zeebe.msgpack.property.LongProperty;
import io.camunda.zeebe.msgpack.property.StringProperty;
import io.camunda.zeebe.msgpack.value.LongValue;
import io.camunda.zeebe.msgpack.value.StringValue;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PersistedMapping extends UnpackedObject implements DbValue {

  private final LongProperty mappingKeyProp = new LongProperty("mappingKey", -1L);
  private final StringProperty claimNameProp = new StringProperty("claimName", "");
  private final StringProperty claimValueProp = new StringProperty("claimValue", "");
  private final ArrayProperty<LongValue> roleKeysProp =
      new ArrayProperty<>("roleKeys", LongValue::new);
  private final ArrayProperty<StringValue> tenantIdsProp =
      new ArrayProperty<>("tenantIds", StringValue::new);

  public PersistedMapping() {
    super(5);
    declareProperty(mappingKeyProp);
    declareProperty(claimNameProp);
    declareProperty(claimValueProp);
    declareProperty(roleKeysProp);
    declareProperty(tenantIdsProp);
  }

  public long getMappingKey() {
    return mappingKeyProp.getValue();
  }

  public PersistedMapping setMappingKey(final long mappingKey) {
    mappingKeyProp.setValue(mappingKey);
    return this;
  }

  public String getClaimName() {
    return BufferUtil.bufferAsString(claimNameProp.getValue());
  }

  public PersistedMapping setClaimName(final String claimName) {
    claimNameProp.setValue(claimName);
    return this;
  }

  public String getClaimValue() {
    return BufferUtil.bufferAsString(claimValueProp.getValue());
  }

  public PersistedMapping setClaimValue(final String claimValue) {
    claimValueProp.setValue(claimValue);
    return this;
  }

  public List<Long> getRoleKeysList() {
    return StreamSupport.stream(roleKeysProp.spliterator(), false)
        .map(LongValue::getValue)
        .collect(Collectors.toList());
  }

  public PersistedMapping setRoleKeysList(final List<Long> roleKeys) {
    roleKeysProp.reset();
    roleKeys.forEach(roleKey -> roleKeysProp.add().setValue(roleKey));
    return this;
  }

  public PersistedMapping addRoleKey(final long roleKey) {
    roleKeysProp.add().setValue(roleKey);
    return this;
  }

  public List<String> getTenantIdsList() {
    return StreamSupport.stream(tenantIdsProp.spliterator(), false)
        .map(StringValue::getValue)
        .map(BufferUtil::bufferAsString)
        .collect(Collectors.toList());
  }

  public PersistedMapping setTenantIdsList(final List<String> tenantIds) {
    tenantIdsProp.reset();
    tenantIds.forEach(id -> tenantIdsProp.add().wrap(BufferUtil.wrapString(id)));
    return this;
  }

  public PersistedMapping addTenantId(final String tenantId) {
    tenantIdsProp.add().wrap(BufferUtil.wrapString(tenantId));
    return this;
  }
}
