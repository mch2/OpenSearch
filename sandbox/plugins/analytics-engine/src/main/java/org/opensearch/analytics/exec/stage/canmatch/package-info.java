/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Coordinator-side can-match pre-filter stage. Inserted by
 * {@link org.opensearch.analytics.planner.dag.DAGBuilder} as a child of any shard
 * fragment stage that carries extractable range predicates; runs a parallel
 * {@link org.opensearch.analytics.exec.canmatch.CanMatchPreFilterPhase} dispatch
 * and publishes the surviving target list as stage metadata so the parent shard
 * stage scans only matching shards.
 */
package org.opensearch.analytics.exec.stage.canmatch;
