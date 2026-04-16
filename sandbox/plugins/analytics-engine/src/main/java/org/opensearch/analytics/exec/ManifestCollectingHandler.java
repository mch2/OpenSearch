/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.backend.FragmentExecutionResponse;
import org.opensearch.core.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link StageResultHandler} for shuffle-write stages. Collects partition
 * manifests from each shard's metadata response instead of feeding a sink.
 *
 * <p>After the stage completes, {@link #getManifests()} returns the collected
 * manifests keyed by shard ID.
 *
 * @opensearch.internal
 */
public class ManifestCollectingHandler implements StageResultHandler {

    private final Map<ShardId, Map<Integer, String>> manifests = new ConcurrentHashMap<>();

    @Override
    public void onBatch(FragmentExecutionResponse response, ShardTarget target) {
        manifests.put(target.shardId(), parseManifest(response.getMetadata()));
    }

    /** Returns the collected manifests after all shards have responded. */
    public Map<ShardId, Map<Integer, String>> getManifests() {
        return manifests;
    }

    private static Map<Integer, String> parseManifest(Map<String, String> metadata) {
        Map<Integer, String> manifest = new HashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            manifest.put(Integer.parseInt(entry.getKey()), entry.getValue());
        }
        return manifest;
    }
}
