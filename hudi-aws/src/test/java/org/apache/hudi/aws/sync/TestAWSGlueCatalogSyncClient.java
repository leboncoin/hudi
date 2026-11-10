/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.aws.sync;

import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.sync.common.HoodieMetaSyncOperations.HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC;
import static org.apache.hudi.sync.common.HoodieMetaSyncOperations.HOODIE_LAST_COMMIT_TIME_SYNC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAWSGlueCatalogSyncClient {

  // Two writers on the same table: the ingress starts first but finishes last.
  private static final String INGRESS_INSTANT = "20261106170927748";
  private static final String INGRESS_COMPLETION = "20261106171549000";
  private static final String PSEUDO_INSTANT = "20261106171107627";
  private static final String PSEUDO_COMPLETION = "20261106171309000";

  private static HoodieInstant completedCommit(String instantTime, String completionTime) {
    return new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, instantTime, completionTime);
  }

  private static Map<String, String> params(String lastCommitTime, String completionTime) {
    Map<String, String> params = new HashMap<>();
    params.put(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitTime);
    params.put(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC, completionTime);
    return params;
  }

  @Test
  public void testSyncedCommitParamsKeepsLatestCompletionTime() {
    HoodieTimeline timeline = new HoodieDefaultTimeline(
        Stream.of(completedCommit(INGRESS_INSTANT, INGRESS_COMPLETION), completedCommit(PSEUDO_INSTANT, PSEUDO_COMPLETION)),
        instant -> Option.empty());

    Map<String, String> synced = AWSGlueCatalogSyncClient.syncedCommitParams(timeline);

    // Last commit time comes from the latest instant, completion time from the commit that finished last.
    assertEquals(PSEUDO_INSTANT, synced.get(HOODIE_LAST_COMMIT_TIME_SYNC));
    assertEquals(INGRESS_COMPLETION, synced.get(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC));
  }

  @Test
  public void testSyncedCommitParamsOnEmptyTimeline() {
    HoodieTimeline timeline = new HoodieDefaultTimeline(Stream.empty(), instant -> Option.empty());

    assertTrue(AWSGlueCatalogSyncClient.syncedCommitParams(timeline).isEmpty());
  }

  @Test
  public void testValidCompletionTimeSynced() {
    // Value written with the last commit time: kept, even when it is newer than that commit.
    assertEquals(Option.of(INGRESS_COMPLETION),
        AWSGlueCatalogSyncClient.validCompletionTimeSynced(params(PSEUDO_INSTANT, INGRESS_COMPLETION)));
    // Never written: nothing to use.
    assertEquals(Option.empty(),
        AWSGlueCatalogSyncClient.validCompletionTimeSynced(Collections.singletonMap(HOODIE_LAST_COMMIT_TIME_SYNC, PSEUDO_INSTANT)));
    // No last commit time to compare with: kept.
    assertEquals(Option.of(INGRESS_COMPLETION),
        AWSGlueCatalogSyncClient.validCompletionTimeSynced(Collections.singletonMap(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC, INGRESS_COMPLETION)));
  }

  @Test
  public void testValidCompletionTimeSyncedIgnoresStaleValue() {
    // Older than the last synced commit, so left by a sync that did not keep it up to date.
    assertEquals(Option.empty(),
        AWSGlueCatalogSyncClient.validCompletionTimeSynced(params(PSEUDO_INSTANT, "20260219092208000")));
  }

  @Test
  public void testSanitizeForGlue() {
    assertNull(AWSGlueCatalogSyncClient.sanitizeForGlue(null));
    assertEquals("line1 line2", AWSGlueCatalogSyncClient.sanitizeForGlue("line1\nline2"));
    assertEquals("line1 line2", AWSGlueCatalogSyncClient.sanitizeForGlue("line1\rline2"));
    assertEquals("line1line2", AWSGlueCatalogSyncClient.sanitizeForGlue("line1\u0000line2"));
    assertEquals("line1 line2", AWSGlueCatalogSyncClient.sanitizeForGlue("line1\u000Bline2"));
    assertEquals("keep\ttab", AWSGlueCatalogSyncClient.sanitizeForGlue("keep\ttab"));
    assertEquals("smile \uD83D\uDE00", AWSGlueCatalogSyncClient.sanitizeForGlue("smile \uD83D\uDE00"));
    assertEquals("ab", AWSGlueCatalogSyncClient.sanitizeForGlue("a\uD800b"));
    assertEquals("ab", AWSGlueCatalogSyncClient.sanitizeForGlue("a\uDC00b"));
  }

  @Test
  public void testGlueValidationRegexCompatibility() {
    assertTrue(AWSGlueCatalogSyncClient.isValidGlueString("plain text\twith tab"));
    assertFalse(AWSGlueCatalogSyncClient.isValidGlueString("line1\nline2"));
    assertTrue(AWSGlueCatalogSyncClient.isValidGlueString("emoji \uD83D\uDE00"));
    assertFalse(AWSGlueCatalogSyncClient.isValidGlueString("bad\uD800"));
  }

  @Test
  public void testTableDescription() {
    assertEquals("doc", AWSGlueCatalogSyncClient.tableDescription("doc", "old"));
    assertEquals("manual", AWSGlueCatalogSyncClient.tableDescription(null, "manual"));
    assertEquals("manual", AWSGlueCatalogSyncClient.tableDescription("", "manual"));
    assertNull(AWSGlueCatalogSyncClient.tableDescription(null, null));

    String truncated = AWSGlueCatalogSyncClient.tableDescription(String.join("", Collections.nCopies(3000, "a")), null);
    assertEquals(2048, truncated.length());
    assertTrue(truncated.endsWith(" [truncated]"));
  }
}
