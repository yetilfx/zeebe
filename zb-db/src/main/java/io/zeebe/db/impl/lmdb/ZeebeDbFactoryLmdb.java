package io.zeebe.db.impl.lmdb;

import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.util.ByteValue;
import java.io.File;
import java.io.IOException;
import org.agrona.DirectBuffer;
import org.lmdbjava.DirectBufferProxy;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;

public class ZeebeDbFactoryLmdb<ColumnFamilyNames extends Enum<ColumnFamilyNames>>
    implements ZeebeDbFactory<ColumnFamilyNames> {

  private final Class<ColumnFamilyNames> columnFamilyTypeClass;

  private ZeebeDbFactoryLmdb(Class<ColumnFamilyNames> columnFamilyTypeClass) {
    this.columnFamilyTypeClass = columnFamilyTypeClass;
  }

  public static <ColumnFamilyType extends Enum<ColumnFamilyType>>
      ZeebeDbFactory<ColumnFamilyType> newFactory(Class<ColumnFamilyType> columnFamilyTypeClass) {
    return new ZeebeDbFactoryLmdb<>(columnFamilyTypeClass);
  }

  @Override
  public ZeebeDb<ColumnFamilyNames> createDb(File pathName) {
    if (!pathName.exists()) {
      try {
        pathName.createNewFile();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    final Env<DirectBuffer> environment =
        Env.create(DirectBufferProxy.PROXY_DB)
            .setMapSize(ByteValue.ofMegabytes(256).toBytes())
            .setMaxDbs(columnFamilyTypeClass.getEnumConstants().length)
            .open(pathName, 0664, EnvFlags.MDB_NOLOCK);

    return new ZeebeDbLmdb<>(environment);
  }
}
