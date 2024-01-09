/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.codec;

import org.apache.lucene.backward_codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugin.correlation.core.index.codec.correlation950.CorrelationCodec;
import org.opensearch.plugin.correlation.core.index.codec.correlation950.PerFieldCorrelation95VectorsFormat;
import org.opensearch.plugin.correlation.core.index.codec.correlation990.Correlation99Codec;
import org.opensearch.plugin.correlation.core.index.codec.correlation990.PerFieldCorrelation99VectorsFormat;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * CorrelationCodecVersion enum
 *
 * @opensearch.internal
 */
public enum CorrelationCodecVersion {

    V_9_5_0(
        "CorrelationCodec",
        new Lucene95Codec(),
        new PerFieldCorrelation95VectorsFormat(Optional.empty()),
        (userCodec, mapperService) -> new CorrelationCodec(userCodec, new PerFieldCorrelation95VectorsFormat(Optional.of(mapperService))),
        CorrelationCodec::new
    ),
    V_9_9_0(
        "Correlation99Codec",
        new Lucene99Codec(),
        new PerFieldCorrelation95VectorsFormat(Optional.empty()),
        (userCodec, mapperService) -> new Correlation99Codec(userCodec, new PerFieldCorrelation99VectorsFormat(Optional.of(mapperService))),
        Correlation99Codec::new
    );

    private static final CorrelationCodecVersion CURRENT = V_9_9_0;
    private final String codecName;
    private final Codec defaultCodecDelegate;
    private final KnnVectorsFormat perFieldCorrelationVectorsFormat;
    private final BiFunction<Codec, MapperService, Codec> correlationCodecSupplier;
    private final Supplier<Codec> defaultCorrelationCodecSupplier;

    CorrelationCodecVersion(
        String codecName,
        Codec defaultCodecDelegate,
        KnnVectorsFormat perFieldCorrelationVectorsFormat,
        BiFunction<Codec, MapperService, Codec> correlationCodecSupplier,
        Supplier<Codec> defaultCorrelationCodecSupplier
    ) {
        this.codecName = codecName;
        this.defaultCodecDelegate = defaultCodecDelegate;
        this.perFieldCorrelationVectorsFormat = perFieldCorrelationVectorsFormat;
        this.correlationCodecSupplier = correlationCodecSupplier;
        this.defaultCorrelationCodecSupplier = defaultCorrelationCodecSupplier;
    }

    /**
     * get codec name
     * @return codec name
     */
    public String getCodecName() {
        return codecName;
    }

    /**
     * get default codec delegate
     * @return default codec delegate
     */
    public Codec getDefaultCodecDelegate() {
        return defaultCodecDelegate;
    }

    /**
     * get correlation vectors format
     * @return correlation vectors format
     */
    public KnnVectorsFormat getPerFieldCorrelationVectorsFormat() {
        return perFieldCorrelationVectorsFormat;
    }

    /**
     * get correlation codec supplier
     * @return correlation codec supplier
     */
    public BiFunction<Codec, MapperService, Codec> getCorrelationCodecSupplier() {
        return correlationCodecSupplier;
    }

    /**
     * get default correlation codec supplier
     * @return default correlation codec supplier
     */
    public Supplier<Codec> getDefaultCorrelationCodecSupplier() {
        return defaultCorrelationCodecSupplier;
    }

    /**
     * static method to get correlation codec version
     * @return correlation codec version
     */
    public static final CorrelationCodecVersion current() {
        return CURRENT;
    }
}
