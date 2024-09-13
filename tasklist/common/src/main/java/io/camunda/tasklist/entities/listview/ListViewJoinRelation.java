/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.tasklist.entities.listview;

import com.fasterxml.jackson.annotation.JsonInclude;

public class ListViewJoinRelation {

  private String name;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long parent;

  public ListViewJoinRelation() {}

  public ListViewJoinRelation(final String name) {

    this.name = name;
  }

  public ListViewJoinRelation(final String name, final Long parent) {
    this.name = name;
    this.parent = parent;
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public Long getParent() {
    return parent;
  }

  public void setParent(final Long parent) {
    this.parent = parent;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (parent != null ? parent.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ListViewJoinRelation that = (ListViewJoinRelation) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    return parent != null ? parent.equals(that.parent) : that.parent == null;
  }
}
