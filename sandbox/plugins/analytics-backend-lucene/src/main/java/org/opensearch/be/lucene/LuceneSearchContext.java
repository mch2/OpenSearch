/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.List;

/**
 * Lucene-specific search context. Holds the reader, query, and lazily-prepared
 * Weight/leaves. Shared between {@link LuceneSearchExecEngine} (execute mode)
 * and {@link LuceneFilterDelegationTarget} (delegation mode).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchContext {

    private final DirectoryReader reader;
    private final IndexSearcher indexSearcher;
    private Query query;
    private Weight weight;
    private List<LeafReaderContext> leaves;

    public LuceneSearchContext(DirectoryReader reader) {
        this.reader = reader;
        this.indexSearcher = new IndexSearcher(reader);
    }

    public DirectoryReader getReader() {
        return reader;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
        // Reset prepared state when query changes
        this.weight = null;
        this.leaves = null;
    }

    /**
     * Lazily prepares the Weight and leaf contexts from the current query.
     * Safe to call multiple times — only prepares once per query.
     */
    public void ensureWeightPrepared() throws IOException {
        if (weight == null) {
            if (query == null) {
                throw new IllegalStateException("No query set on LuceneSearchContext");
            }
            Query rewritten = indexSearcher.rewrite(query);
            this.weight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            this.leaves = reader.leaves();
        }
    }

    public Weight getWeight() {
        return weight;
    }

    public List<LeafReaderContext> getLeaves() {
        return leaves;
    }

    public IndexSearcher getIndexSearcher() {
        return indexSearcher;
    }

    public void close() throws IOException {
        // Reader lifecycle is owned by the ReaderManager, not the context
    }
}
