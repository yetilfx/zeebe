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

import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.util.ByteValue;
import java.io.File;
import org.agrona.DirectBuffer;
import org.lmdbjava.DirectBufferProxy;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;

public final class ZeebeDbFactoryLmdb<ColumnFamilyNames extends Enum<ColumnFamilyNames>>
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
      pathName.mkdirs();
    }

    final Env<DirectBuffer> environment =
        Env.create(DirectBufferProxy.PROXY_DB)
            .setMapSize(ByteValue.ofMegabytes(256).toBytes())
            .setMaxDbs(columnFamilyTypeClass.getEnumConstants().length)
            .open(pathName, 0664, EnvFlags.MDB_NOLOCK);

    return new ZeebeDbLmdb<>(environment);
  }
}
