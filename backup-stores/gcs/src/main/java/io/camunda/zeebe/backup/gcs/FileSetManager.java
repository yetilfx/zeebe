/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.backup.gcs;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import io.camunda.zeebe.backup.api.BackupIdentifier;
import io.camunda.zeebe.backup.api.NamedFileSet;
import java.io.IOException;
import java.io.UncheckedIOException;

final class FileSetManager {
  private static final String FILESET_PREFIX = "contents/";
  private final Storage client;
  private final BucketInfo bucketInfo;
  private final String prefix;

  FileSetManager(final Storage client, final BucketInfo bucketInfo, final String prefix) {
    this.client = client;
    this.bucketInfo = bucketInfo;
    this.prefix = prefix + FILESET_PREFIX;
  }

  void save(final BackupIdentifier id, final String fileSetName, final NamedFileSet fileSet) {
    for (final var namedFile : fileSet.namedFiles().entrySet()) {
      final var fileName = namedFile.getKey();
      final var filePath = namedFile.getValue();
      try {
        client.createFrom(
            blobInfo(id, fileSetName, fileName),
            filePath,
            BlobWriteOption.doesNotExist(),
            BlobWriteOption.crc32cMatch());
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  public void delete(final BackupIdentifier id, final String fileSetName) {
    for (final var blob :
        client
            .list(bucketInfo.getName(), BlobListOption.prefix(fileSetPath(id, fileSetName)))
            .iterateAll()) {
      blob.delete();
    }
  }

  private String fileSetPath(final BackupIdentifier id, final String fileSetName) {
    return prefix
        + "%s/%s/%s/".formatted(id.partitionId(), id.checkpointId(), id.nodeId())
        + fileSetName
        + "/";
  }

  private BlobInfo blobInfo(
      final BackupIdentifier id, final String fileSetName, final String fileName) {
    return BlobInfo.newBuilder(bucketInfo, fileSetPath(id, fileSetName) + fileName)
        .setContentType("application/octet-stream")
        .build();
  }
}
