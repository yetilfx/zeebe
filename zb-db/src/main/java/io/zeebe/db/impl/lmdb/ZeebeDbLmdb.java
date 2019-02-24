package io.zeebe.db.impl.lmdb;

import static io.zeebe.util.buffer.BufferUtil.startsWith;

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
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.lmdbjava.CopyFlags;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;

public class ZeebeDbLmdb<ColumnFamilyType extends Enum<ColumnFamilyType>>
  implements ZeebeDb<ColumnFamilyType> {

  private final Env<DirectBuffer> env;

  private final ExpandableArrayBuffer keyBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer valueBuffer = new ExpandableArrayBuffer();
  private final ExpandableArrayBuffer prefixBuffer = new ExpandableArrayBuffer();

  private final DirectBuffer keyViewBuffer = new UnsafeBuffer(0, 0);
  private final DirectBuffer valueViewBuffer = new UnsafeBuffer(0, 0);
  private final DirectBuffer prefixViewBuffer = new UnsafeBuffer(0, 0);

  // todo: investigate if we could keep a write transaction around for a long time?
  private Txn<DirectBuffer> transaction;

  public ZeebeDbLmdb(Env<DirectBuffer> env) {
    this.env = env;
    this.transaction = env.txnWrite();
  }

  // todo: implement with cursor
  @Override
  public void batch(Runnable operations) {
    try {
      operations.run();
      transaction.commit();
    } finally {
      transaction.close();
      transaction = env.txnWrite();
    }
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
    dbHandle.put(serializedKey, serializedValue);
  }

  public DirectBuffer get(Dbi<DirectBuffer> dbHandle, DbKey key) {
    final DirectBuffer serializedKey = writeKey(key);
    return dbHandle.get(transaction, serializedKey);
  }

  public void delete(Dbi<DirectBuffer> dbHandle, DbKey key) {
    final DirectBuffer serializedKey = writeKey(key);
    dbHandle.delete(transaction, serializedKey);
  }

  public void delete(Dbi<DirectBuffer> dbHandle, DbKey key, DbValue value) {
    final DirectBuffer serializedKey = writeKey(key);
    final DirectBuffer serializedValue = writeValue(value);
    dbHandle.delete(transaction, serializedKey, serializedValue);
  }

  public boolean exists(Dbi<DirectBuffer> dbHandle, DbKey key) {
    return get(dbHandle, key) != null;
  }

  public boolean isEmpty(Dbi<DirectBuffer> dbHandle) {
    return dbHandle.stat(transaction).entries == 0;
  }

  public <V extends DbValue> void forEach(
    Dbi<DirectBuffer> dbHandle, V valueInstance, Consumer<V> consumer) {
    try (CursorIterator<DirectBuffer> iterator = dbHandle.iterate(transaction, KeyRange.all())) {
      for (final KeyVal<DirectBuffer> kv : iterator.iterable()) {
        valueInstance.wrap(kv.val(), 0, kv.val().capacity());
        consumer.accept(valueInstance);
      }
    }
  }

  public <K extends DbKey, V extends DbValue> void forEach(
    Dbi<DirectBuffer> dbHandle, K keyInstance, V valueInstance, BiConsumer<K, V> consumer) {
    try (CursorIterator<DirectBuffer> iterator = dbHandle.iterate(transaction, KeyRange.all())) {
      for (final KeyVal<DirectBuffer> kv : iterator.iterable()) {
        keyInstance.wrap(kv.key(), 0, kv.key().capacity());
        valueInstance.wrap(kv.val(), 0, kv.val().capacity());
        consumer.accept(keyInstance, valueInstance);
      }
    }
  }

  public <K extends DbKey, V extends DbValue> void whileTrue(
    Dbi<DirectBuffer> dbHandle,
    K keyInstance,
    V valueInstance,
    KeyValuePairVisitor<K, V> visitor) {
    try (CursorIterator<DirectBuffer> iterator = dbHandle.iterate(transaction, KeyRange.all())) {
      for (final KeyVal<DirectBuffer> kv : iterator.iterable()) {
        keyInstance.wrap(kv.key(), 0, kv.key().capacity());
        valueInstance.wrap(kv.val(), 0, kv.val().capacity());
        if (!visitor.visit(keyInstance, valueInstance)) {
          break;
        }
      }
    }
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

    try (CursorIterator<DirectBuffer> iterator =
      dbHandle.iterate(transaction, KeyRange.atLeast(serializedPrefix))) {
      for (final KeyVal<DirectBuffer> kv : iterator.iterable()) {
        if (BufferUtil.startsWith(kv.key(), serializedPrefix)) {
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
}
