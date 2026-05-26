/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

/**
 * Typed control-plane payload published by a stage on SUCCEEDED and delivered to its
 * parent via {@link StageExecution#consumeChildMetadata}. Distinct from data flow
 * (which goes through {@code ExchangeSink}); metadata is for cross-stage coordination
 * decisions like "which shards survived can-match pruning".
 *
 * <p>TODO: convert to {@code sealed interface ... permits CanMatchManifest, ...}
 * once a second metadata type lands so the compiler can enforce exhaustive handling
 * via pattern-matching switches. Today the only consumer (ShardFragmentStageExecution)
 * already uses a typed instanceof on {@link CanMatchManifest}, so the runtime check
 * is tight; the seal would harden it at compile time.
 *
 * @opensearch.internal
 */
public interface StageMetadata {}
