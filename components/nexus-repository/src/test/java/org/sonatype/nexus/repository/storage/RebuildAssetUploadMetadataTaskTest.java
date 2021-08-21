/*
 * Sonatype Nexus (TM) Open Source Version
 * Copyright (c) 2008-present Sonatype, Inc.
 * All rights reserved. Includes the third-party code listed at http://links.sonatype.com/products/nexus/oss/attributions.
 *
 * This program and the accompanying materials are made available under the terms of the Eclipse Public License Version 1.0,
 * which accompanies this distribution and is available at http://www.eclipse.org/legal/epl-v10.html.
 *
 * Sonatype Nexus (TM) Professional Version is available from Sonatype, Inc. "Sonatype" and "Sonatype Nexus" are trademarks
 * of Sonatype, Inc. Apache Maven is a trademark of the Apache Software Foundation. M2eclipse is a trademark of the
 * Eclipse Foundation. All other trademarks are the property of their respective owners.
 */
package org.sonatype.nexus.repository.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.sonatype.goodies.testsupport.TestSupport;
import org.sonatype.nexus.blobstore.api.Blob;
import org.sonatype.nexus.blobstore.api.BlobId;
import org.sonatype.nexus.blobstore.api.BlobMetrics;
import org.sonatype.nexus.blobstore.api.BlobRef;
import org.sonatype.nexus.blobstore.api.BlobStore;
import org.sonatype.nexus.blobstore.api.BlobStoreManager;
import org.sonatype.nexus.common.collect.NestedAttributesMap;
import org.sonatype.nexus.orient.testsupport.DatabaseInstanceRule;
import org.sonatype.nexus.scheduling.TaskInterruptedException;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;

import static java.util.Collections.emptySet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.sonatype.nexus.common.entity.EntityHelper.id;
import static org.sonatype.nexus.repository.storage.MetadataNodeEntityAdapter.P_ATTRIBUTES;

public class RebuildAssetUploadMetadataTaskTest
    extends TestSupport
{
  private static final int PAGE_SIZE = 1;
  private static final String REPOSITORY_NAME = "repositoryName";

  private RebuildAssetUploadMetadataTask task;

  @Rule
  public DatabaseInstanceRule database = DatabaseInstanceRule.inMemory("RebuildAssetUploadMetadataTaskTest");

  private RebuildAssetUploadMetadataConfiguration configuration = new RebuildAssetUploadMetadataConfiguration(true,
      PAGE_SIZE);

  private AssetStore assetStore;

  private AssetEntityAdapter assetEntityAdapter;

  private Bucket bucket;

  @Mock
  private BlobStoreManager blobStoreManager;

  @Mock
  private BlobStore blobStore;

  private int assets = 0;

  @Before
  public void setUp() {
    BucketEntityAdapter bucketEntityAdapter = new BucketEntityAdapter();
    ComponentFactory componentFactory = new ComponentFactory(emptySet());
    ComponentEntityAdapter componentEntityAdapter = new ComponentEntityAdapter(bucketEntityAdapter, componentFactory,
        emptySet());
    assetEntityAdapter = new AssetEntityAdapter(bucketEntityAdapter, componentEntityAdapter);

    try (ODatabaseDocumentTx db = database.getInstance().connect()) {
      bucketEntityAdapter.register(db);
      componentEntityAdapter.register(db);
      assetEntityAdapter.register(db);

      bucket = bucketEntityAdapter.newEntity();
      bucket.attributes(new NestedAttributesMap(P_ATTRIBUTES, new HashMap<>()));
      bucket.setRepositoryName(REPOSITORY_NAME);
      bucketEntityAdapter.addEntity(db, bucket);
    }

    assetStore = new AssetStoreImpl(database.getInstanceProvider(), assetEntityAdapter);

    task = new RebuildAssetUploadMetadataTask(assetStore, blobStoreManager, configuration);
    when(blobStoreManager.get(any())).thenReturn(blobStore);
  }

  @Test(expected = TaskInterruptedException.class)
  public void executeCancelledTaskThrowsTaskInterruptedException() {
    task.cancel();

    Blob blob = createMockBlob("blobId");
    Asset asset = createAsset(blob);
    assetStore.save(asset);

    task.execute();
  }

  @Test
  public void executeUpdatesCandidateAssets() {
    Blob blob = createMockBlob("blobId");
    Asset asset = createAsset(blob);
    assetStore.save(asset);

    task.execute();

    Asset updatedAsset = assetStore.getById(id(asset));
    assertThat(updatedAsset.name(), is(asset.name()));
    assertThat(updatedAsset.blobCreated(), is(blob.getMetrics().getCreationTime()));
  }

  @Test
  public void executeSkipsAssetsWithANonEmptyCreatedBy() {
    Blob blob = createMockBlob("blobId");
    Asset asset = createAsset(blob);
    assetStore.save(asset);

    task.execute();

    Asset updatedAsset = assetStore.getById(id(asset));
    assertThat(updatedAsset.name(), is(asset.name()));
  }

  @Test
  public void executeSkipsAssetsWithANoBlobRef() {
    Asset assetWithNoBlobRef = createAsset(null);
    assetStore.save(assetWithNoBlobRef);

    task.execute();

    Asset updatedAssetWithNoBlobRef = assetStore.getById(id(assetWithNoBlobRef));
    assertThat(updatedAssetWithNoBlobRef.name(), is(assetWithNoBlobRef.name()));
  }

  private Asset createAsset(Blob blob) {
    Asset asset = new Asset();
    asset.bucketId(id(bucket));
    asset.attributes(new NestedAttributesMap(P_ATTRIBUTES, new HashMap<>()));
    if (blob != null) {
      asset.blobRef(new BlobRef("node", "store", blob.getId().asUniqueString()));
    }
    asset.format("format");
    asset.name("asset" + (assets++));
    return asset;
  }

  private Blob createMockBlob(String id) {
    DateTime creationTime = new DateTime();
    Blob blob = mock(Blob.class);

    Map<String, String> headers = new HashMap<>();
    when(blob.getHeaders()).thenReturn(headers);

    BlobMetrics metrics = new BlobMetrics(creationTime, "hash", 1L);
    when(blob.getMetrics()).thenReturn(metrics);

    BlobId blobId = new BlobId(id);
    when(blob.getId()).thenReturn(blobId);
    when(blobStore.get(blobId)).thenReturn(blob);

    return blob;
  }
}
