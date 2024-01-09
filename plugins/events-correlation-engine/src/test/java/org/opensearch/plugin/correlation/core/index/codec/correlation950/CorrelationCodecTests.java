/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.codec.correlation950;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugin.correlation.core.index.CorrelationParamsContext;

import org.opensearch.plugin.correlation.core.index.codec.CorrelationCodecService;
import org.opensearch.plugin.correlation.core.index.codec.CorrelationCodecVersion;
import org.opensearch.plugin.correlation.core.index.codec.correlation990.Correlation99Codec;
import org.opensearch.plugin.correlation.core.index.mapper.VectorFieldMapper;
import org.opensearch.plugin.correlation.core.index.query.CorrelationQueryFactory;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.opensearch.plugin.correlation.core.index.codec.BasePerFieldCorrelationVectorsFormat.METHOD_PARAMETER_EF_CONSTRUCTION;
import static org.opensearch.plugin.correlation.core.index.codec.BasePerFieldCorrelationVectorsFormat.METHOD_PARAMETER_M;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for custom correlation codec
 */
public class CorrelationCodecTests extends OpenSearchTestCase {

    private static final String FIELD_NAME_ONE = "test_vector_one";
    private static final String FIELD_NAME_TWO = "test_vector_two";

    /**
     * test correlation vector index
     * @throws Exception Exception
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8329")
    public void testCorrelationVectorIndex() throws Exception {
        Function<MapperService, PerFieldCorrelation95VectorsFormat> perFieldCorrelationVectorsProvider =
            mapperService -> new PerFieldCorrelation95VectorsFormat(Optional.of(mapperService));
        Function<PerFieldCorrelation95VectorsFormat, Codec> correlationCodecProvider = (correlationVectorsFormat -> new Correlation99Codec(
            CorrelationCodecVersion.current().getDefaultCodecDelegate(),
            correlationVectorsFormat
        ));
        testCorrelationVectorIndex(correlationCodecProvider, perFieldCorrelationVectorsProvider);
    }

    public void testBwcWith95Codec() throws IOException {
        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig iwc = newIndexWriterConfig();
        final MapperService mapperService = mock(MapperService.class);
        CorrelationCodecService service = new CorrelationCodecService(new CodecServiceConfig(IndexSettingsModule.newIndexSettings("index", Settings.EMPTY), mapperService, null));
        // write segments with old (95) codec
        iwc.setCodec(service.codec(CorrelationCodecVersion.current().getCodecName()));
        final FieldType luceneFieldType = KnnFloatVectorField.createFieldType(3, VectorSimilarityFunction.EUCLIDEAN);
//        float[] array = { 1.0f, 3.0f, 4.0f };
        KnnFloatVectorField vectorField = new KnnFloatVectorField(FIELD_NAME_ONE, new float[]{1.0f, 3.0f, 4.0f}, luceneFieldType);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
        Document doc = new Document();
        doc.add(vectorField);
        writer.addDocument(doc);
        writer.commit();
        try (DirectoryReader reader = writer.getReader()) {
            SegmentInfos segmentInfos = ((StandardDirectoryReader) reader).getSegmentInfos();
            logger.info("Seg {}", segmentInfos);
            for (SegmentCommitInfo segmentInfo : segmentInfos) {
                logger.info("info {}", segmentInfo.info.getCodec());
            }
        }
        writer.close();

        logger.info("Directory {}", Arrays.stream(dir.listAll()).toArray());
        dir.close();
    }

    private void testCorrelationVectorIndex(
        final Function<PerFieldCorrelation95VectorsFormat, Codec> codecProvider,
        final Function<MapperService, PerFieldCorrelation95VectorsFormat> perFieldCorrelationVectorsProvider
    ) throws Exception {
        final MapperService mapperService = mock(MapperService.class);
        final CorrelationParamsContext correlationParamsContext = new CorrelationParamsContext(
            VectorSimilarityFunction.EUCLIDEAN,
            Map.of(METHOD_PARAMETER_M, 16, METHOD_PARAMETER_EF_CONSTRUCTION, 256)
        );

        final VectorFieldMapper.CorrelationVectorFieldType mappedFieldType1 = new VectorFieldMapper.CorrelationVectorFieldType(
            FIELD_NAME_ONE,
            Map.of(),
            3,
            correlationParamsContext
        );
        final VectorFieldMapper.CorrelationVectorFieldType mappedFieldType2 = new VectorFieldMapper.CorrelationVectorFieldType(
            FIELD_NAME_TWO,
            Map.of(),
            2,
            correlationParamsContext
        );
        when(mapperService.fieldType(eq(FIELD_NAME_ONE))).thenReturn(mappedFieldType1);
        when(mapperService.fieldType(eq(FIELD_NAME_TWO))).thenReturn(mappedFieldType2);

        var perFieldCorrelationVectorsFormatSpy = spy(perFieldCorrelationVectorsProvider.apply(mapperService));
        final Codec codec = codecProvider.apply(perFieldCorrelationVectorsFormatSpy);

        Directory dir = newFSDirectory(createTempDir());
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setCodec(codec);

        final FieldType luceneFieldType = KnnFloatVectorField.createFieldType(3, VectorSimilarityFunction.EUCLIDEAN);
        float[] array = { 1.0f, 3.0f, 4.0f };
        KnnFloatVectorField vectorField = new KnnFloatVectorField(FIELD_NAME_ONE, array, luceneFieldType);
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc);
        Document doc = new Document();
        doc.add(vectorField);
        writer.addDocument(doc);
        writer.commit();
        IndexReader reader = writer.getReader();
        writer.close();

        verify(perFieldCorrelationVectorsFormatSpy).getKnnVectorsFormatForField(eq(FIELD_NAME_ONE));

        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = CorrelationQueryFactory.create(
            new CorrelationQueryFactory.CreateQueryRequest("dummy", FIELD_NAME_ONE, new float[] { 1.0f, 0.0f, 0.0f }, 1, null, null)
        );

        assertEquals(1, searcher.count(query));

        reader.close();
        dir.close();
    }

}
