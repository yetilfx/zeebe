/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.db.impl.lmdb;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbKey;
import io.zeebe.db.DbValue;
import io.zeebe.db.KeyValuePairVisitor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.lmdbjava.Dbi;

public class ColumnFamilyLmdb<KeyType extends DbKey, ValueType extends DbValue>
    implements ColumnFamily<KeyType, ValueType> {

  private final Dbi<DirectBuffer> dbHandle;
  private final ZeebeDbLmdb lmdb;
  private final KeyType keyInstance;
  private final ValueType valueInstance;

  public ColumnFamilyLmdb(
      ZeebeDbLmdb lmdb, Dbi<DirectBuffer> dbHandle, KeyType keyInstance, ValueType valueInstance) {
    this.dbHandle = dbHandle;
    this.lmdb = lmdb;
    this.keyInstance = keyInstance;
    this.valueInstance = valueInstance;
  }

  @Override
  public void put(KeyType key, ValueType value) {
    lmdb.put(dbHandle, key, value);
  }

  @Override
  public ValueType get(KeyType key) {
    ValueType value = null;
    final DirectBuffer serializedValue = lmdb.get(dbHandle, key);

    if (serializedValue != null) {
      valueInstance.wrap(serializedValue, 0, serializedValue.capacity());
      value = valueInstance;
    }

    return value;
  }

  @Override
  public void forEach(Consumer<ValueType> consumer) {
    lmdb.forEach(dbHandle, valueInstance, consumer);
  }

  @Override
  public void forEach(BiConsumer<KeyType, ValueType> consumer) {
    lmdb.forEach(dbHandle, keyInstance, valueInstance, consumer);
  }

  @Override
  public void whileTrue(KeyValuePairVisitor<KeyType, ValueType> visitor) {
    lmdb.whileTrue(dbHandle, keyInstance, valueInstance, visitor);
  }

  @Override
  public void whileEqualPrefix(DbKey keyPrefix, BiConsumer<KeyType, ValueType> consumer) {
    lmdb.whileEqualPrefix(dbHandle, keyPrefix, keyInstance, valueInstance, consumer);
  }

  @Override
  public void whileEqualPrefix(DbKey keyPrefix, KeyValuePairVisitor<KeyType, ValueType> visitor) {
    lmdb.whileEqualPrefix(dbHandle, keyPrefix, keyInstance, valueInstance, visitor);
  }

  @Override
  public void delete(KeyType key) {
    lmdb.delete(dbHandle, key);
  }

  @Override
  public boolean exists(KeyType key) {
    return lmdb.exists(dbHandle, key);
  }

  @Override
  public boolean isEmpty() {
    return lmdb.isEmpty(dbHandle);
  }
}
