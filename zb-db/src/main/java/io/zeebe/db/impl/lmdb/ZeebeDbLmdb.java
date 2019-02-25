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
import io.zeebe.db.ZeebeDb;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.buffer.BufferWriter;
import java.io.File;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.lmdbjava.CopyFlags;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

public class ZeebeDbLmdb<ColumnFamilyType extends Enum<ColumnFamilyType>>
    implements ZeebeDb<ColumnFamilyType> {

  private final Env<DirectBuffer> env;

  // little caveat: their DirectBufferProxy only works with off-heap DirectBuffer
  private final MutableDirectBuffer keyBuffer = new ExpandableDirectByteBuffer();
  private final MutableDirectBuffer valueBuffer = new ExpandableDirectByteBuffer();
  private final MutableDirectBuffer prefixBuffer = new ExpandableDirectByteBuffer();

  private final DirectBuffer keyViewBuffer = new UnsafeBuffer(0, 0);
  private final DirectBuffer valueViewBuffer = new UnsafeBuffer(0, 0);
  private final DirectBuffer prefixViewBuffer = new UnsafeBuffer(0, 0);

  // todo: investigate if we could keep a write transaction around for a long time?
  private Txn<DirectBuffer> transaction;

  public ZeebeDbLmdb(Env<DirectBuffer> env) {
    this.env = env;
  }

  // todo: implement with cursor
  @Override
  public void batch(Runnable operations) {
    withTransaction(operations);
  }

  @Override
  public <KeyType extends DbKey, ValueType extends DbValue>
      ColumnFamily<KeyType, ValueType> createColumnFamily(
          ColumnFamilyType columnFamily, KeyType keyInstance, ValueType valueInstance) {
    final Dbi<DirectBuffer> dbHandle = env.openDbi(columnFamily.name(), DbiFlags.MDB_CREATE);
    return new ColumnFamilyLmdb<>(this, dbHandle, keyInstance, valueInstance);
  }

  @Override
  public void createSnapshot(File snapshotDir) {
    env.copy(snapshotDir, CopyFlags.MDB_CP_COMPACT);
  }

  @Override
  public void close() throws Exception {
    env.close();
  }

  public void put(Dbi<DirectBuffer> dbHandle, DbKey key, DbValue value) {
    final DirectBuffer serializedKey = writeKey(key);
    final DirectBuffer serializedValue = writeValue(value);
    withTransaction(() -> dbHandle.put(transaction, serializedKey, serializedValue));
  }

  public DirectBuffer get(Dbi<DirectBuffer> dbHandle, DbKey key) {
    final DirectBuffer serializedKey = writeKey(key);
    final Box<DirectBuffer> serializedValue = new Box<>();
    withTransaction(() -> serializedValue.value = dbHandle.get(transaction, serializedKey));

    return serializedValue.value;
  }

  public void delete(Dbi<DirectBuffer> dbHandle, DbKey key) {
    final DirectBuffer serializedKey = writeKey(key);
    withTransaction(() -> dbHandle.delete(transaction, serializedKey));
  }

  public void delete(Dbi<DirectBuffer> dbHandle, DbKey key, DbValue value) {
    final DirectBuffer serializedKey = writeKey(key);
    final DirectBuffer serializedValue = writeValue(value);
    withTransaction(() -> dbHandle.delete(transaction, serializedKey, serializedValue));
  }

  public boolean exists(Dbi<DirectBuffer> dbHandle, DbKey key) {
    return get(dbHandle, key) != null;
  }

  public boolean isEmpty(Dbi<DirectBuffer> dbHandle) {
    final Box<Boolean> isEmpty = new Box<>();
    withTransaction(() -> isEmpty.value = dbHandle.stat(transaction).entries == 0);
    return isEmpty.value;
  }

  public <V extends DbValue> void forEach(
      Dbi<DirectBuffer> dbHandle, V valueInstance, Consumer<V> consumer) {
    withTransaction(
        () -> {
          try (CursorIterator<DirectBuffer> iterator =
              dbHandle.iterate(transaction, KeyRange.all())) {
            for (final KeyVal<DirectBuffer> kv : iterator.iterable()) {
              valueInstance.wrap(kv.val(), 0, kv.val().capacity());
              consumer.accept(valueInstance);
            }
          }
        });
  }

  public <K extends DbKey, V extends DbValue> void forEach(
      Dbi<DirectBuffer> dbHandle, K keyInstance, V valueInstance, BiConsumer<K, V> consumer) {
    withTransaction(
        () -> {
          try (CursorIterator<DirectBuffer> iterator =
              dbHandle.iterate(transaction, KeyRange.all())) {
            for (final KeyVal<DirectBuffer> kv : iterator.iterable()) {
              keyInstance.wrap(kv.key(), 0, kv.key().capacity());
              valueInstance.wrap(kv.val(), 0, kv.val().capacity());
              consumer.accept(keyInstance, valueInstance);
            }
          }
        });
  }

  public <K extends DbKey, V extends DbValue> void whileTrue(
      Dbi<DirectBuffer> dbHandle,
      K keyInstance,
      V valueInstance,
      KeyValuePairVisitor<K, V> visitor) {
    withTransaction(
        () -> {
          try (CursorIterator<DirectBuffer> iterator =
              dbHandle.iterate(transaction, KeyRange.all())) {
            for (final KeyVal<DirectBuffer> kv : iterator.iterable()) {
              keyInstance.wrap(kv.key(), 0, kv.key().capacity());
              valueInstance.wrap(kv.val(), 0, kv.val().capacity());
              if (!visitor.visit(keyInstance, valueInstance)) {
                break;
              }
            }
          }
        });
  }

  public <K extends DbKey, V extends DbValue> void whileEqualPrefix(
      Dbi<DirectBuffer> dbHandle,
      DbKey prefix,
      K keyInstance,
      V valueInstance,
      BiConsumer<K, V> consumer) {
    whileEqualPrefix(
        dbHandle,
        prefix,
        keyInstance,
        valueInstance,
        (k, v) -> {
          consumer.accept(k, v);
          return true;
        });
  }

  public <K extends DbKey, V extends DbValue> void whileEqualPrefix(
      Dbi<DirectBuffer> dbHandle,
      DbKey prefix,
      K keyInstance,
      V valueInstance,
      KeyValuePairVisitor<K, V> visitor) {
    final DirectBuffer serializedPrefix = writePrefix(prefix);

    withTransaction(
        () -> {
          try (CursorIterator<DirectBuffer> iterator =
              dbHandle.iterate(transaction, KeyRange.atLeast(serializedPrefix))) {
            for (final KeyVal<DirectBuffer> kv : iterator.iterable()) {
              if (BufferUtil.startsWith(serializedPrefix, kv.key())) {
                keyInstance.wrap(kv.key(), 0, kv.key().capacity());
                valueInstance.wrap(kv.val(), 0, kv.val().capacity());

                if (!visitor.visit(keyInstance, valueInstance)) {
                  break;
                }
              } else {
                break;
              }
            }
          }
        });
  }

  protected DirectBuffer writeKey(DbKey key) {
    return write(key, keyBuffer, keyViewBuffer);
  }

  protected DirectBuffer writeValue(DbValue value) {
    return write(value, valueBuffer, valueViewBuffer);
  }

  protected DirectBuffer writePrefix(DbKey prefix) {
    return write(prefix, prefixBuffer, prefixViewBuffer);
  }

  private DirectBuffer write(BufferWriter writer, MutableDirectBuffer dest, DirectBuffer view) {
    writer.write(dest, 0);
    view.wrap(dest, 0, writer.getLength());
    return view;
  }

  private boolean beginTransaction() {
    if (transaction == null) {
      transaction = env.txnWrite();
      return true;
    }

    return false;
  }

  private void withTransaction(Runnable operation) {
    final boolean beganTransaction = beginTransaction();

    try {
      operation.run();

      if (beganTransaction) {
        transaction.commit();
      }
    } finally {
      if (beganTransaction) {
        transaction.close();
        transaction = null;
      }
    }
  }

  static class Box<T> {
    T value;
  }
}
