/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamFieldMapperTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // Required for pluggable-dataformat tests: provide a data-format plugin + committer plugin
        // so that indices with `index.pluggable.dataformat.enabled=true` can be created.
        return pluginList(MockDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    public void testDefaultTimestampField() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertDataStreamFieldMapper(mapping, "@timestamp");
    }

    public void testCustomTimestampField() throws Exception {
        String timestampFieldName = "timestamp_" + randomAlphaOfLength(5);

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .startObject("timestamp_field")
            .field("name", timestampFieldName)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertDataStreamFieldMapper(mapping, timestampFieldName);
    }

    public void testDeeplyNestedCustomTimestampField() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .startObject("timestamp_field")
            .field("name", "event.meta.created_at")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapper mapper = createIndex("test").mapperService()
            .merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("event")
                        .startObject("meta")
                        .field("created_at", "2020-12-06T11:04:05.000Z")
                        .endObject()
                        .endObject()
                        .endObject()
                ),
                MediaTypeRegistry.JSON
            )
        );
        assertThat(doc.rootDoc().getFields("event.meta.created_at").length, equalTo(2));

        MapperException exception = expectThrows(MapperException.class, () -> {
            mapper.parse(
                new SourceToParse(
                    "test",
                    "3",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("event")
                            .startObject("meta")
                            .array("created_at", "2020-12-06T11:04:05.000Z", "2020-12-07T11:04:05.000Z")
                            .endObject()
                            .endObject()
                            .endObject()
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        });
        assertThat(
            exception.getCause().getMessage(),
            containsString("documents must contain a single-valued timestamp field 'event.meta.created_at' of date type")
        );
    }

    /**
     * In pluggable-dataformat mode, DateFieldMapper routes timestamp values to
     * {@code context.documentInput()} rather than {@code context.doc()}, so the
     * SORTED_NUMERIC scan in {@link DataStreamFieldMapper#postParse(ParseContext)}
     * would always count zero and throw. Verify that postParse short-circuits when
     * pluggable-dataformat is enabled so bulk ingest of data-stream-timestamped
     * indices succeeds instead of silently dropping every document.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPostParseSkipsValidationInPluggableDataFormatMode() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        Settings pluggableSettings = Settings.builder().put("index.pluggable.dataformat.enabled", true).build();

        DocumentMapper mapper = createIndex("test-pluggable", pluggableSettings).mapperService()
            .merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        // With pluggable-dataformat, the Lucene primary doc has zero SORTED_NUMERIC
        // fields for @timestamp (the value went to documentInput instead). postParse
        // must not throw in this mode, otherwise bulk drops every doc silently.
        DocumentInput<Object> capturingInput = new DocumentInput<>() {
            @Override
            public Object getFinalInput() {
                return null;
            }

            @Override
            public void addField(MappedFieldType fieldType, Object value) {}

            @Override
            public void setRowId(String rowIdFieldName, long rowId) {}

            @Override
            public void close() {}
        };
        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test-pluggable",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field("@timestamp", "2020-12-06T11:04:05.000Z").endObject()
                ),
                MediaTypeRegistry.JSON
            ),
            capturingInput
        );
        // In pluggable mode the timestamp is not added to rootDoc; confirm postParse did not throw.
        assertThat(doc.rootDoc().getFields("@timestamp").length, equalTo(0));
    }

    private void assertDataStreamFieldMapper(String mapping, String timestampFieldName) throws Exception {
        DocumentMapper mapper = createIndex("test").mapperService()
            .merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        // Success case - document has timestamp field correctly populated.
        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field(timestampFieldName, "2020-12-06T11:04:05.000Z").endObject()
                ),
                MediaTypeRegistry.JSON
            )
        );

        // A valid timestamp field will be parsed as LongPoint and SortedNumericDocValuesField.
        assertThat(doc.rootDoc().getFields(timestampFieldName).length, equalTo(2));

        MapperException exception;

        // Failure case - document doesn't have a valid timestamp field.
        exception = expectThrows(MapperException.class, () -> {
            mapper.parse(
                new SourceToParse(
                    "test",
                    "2",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder().startObject().field("invalid-field-name", "2020-12-06T11:04:05.000Z").endObject()
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        });
        assertThat(
            exception.getCause().getMessage(),
            containsString("documents must contain a single-valued timestamp field '" + timestampFieldName + "' of date type")
        );

        // Failure case - document contains multiple values for the timestamp field.
        exception = expectThrows(MapperException.class, () -> {
            mapper.parse(
                new SourceToParse(
                    "test",
                    "3",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .array(timestampFieldName, "2020-12-06T11:04:05.000Z", "2020-12-07T11:04:05.000Z")
                            .endObject()
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        });
        assertThat(
            exception.getCause().getMessage(),
            containsString("documents must contain a single-valued timestamp field '" + timestampFieldName + "' of date type")
        );
    }

}
