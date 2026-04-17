# Native Arrow Transport Path

## Overview

The Arrow Flight transport supports a native Arrow path where typed `VectorSchemaRoot` data flows directly over Flight without byte serialization. This is useful for APIs that produce Arrow-columnar data natively (e.g., query engines like DataFusion).

## API

### Response

Extend `ArrowBatchResponse` instead of `ActionResponse`:

```java
public class MyQueryResponse extends ArrowBatchResponse {
    public MyQueryResponse(VectorSchemaRoot root) { super(root); }
    public MyQueryResponse(StreamInput in) throws IOException { super(in); }
}
```

The framework handles serialization — `writeTo()` is a no-op, and the constructor from `StreamInput` retrieves the root via `VectorStreamInput.getRoot()`.

### Server-side handler

Get the allocator from the channel, create typed vectors, send batches:

```java
void handleRequest(MyRequest request, TransportChannel channel, Task task) throws IOException {
    BufferAllocator allocator = ArrowFlightChannel.from(channel).getAllocator();
    Schema schema = new Schema(List.of(
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
    ));

    try {
        for (int i = 0; i < batchCount; i++) {
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            // populate vectors...
            channel.sendResponseBatch(new MyQueryResponse(root));
        }
        channel.completeStream();
    } catch (StreamException e) {
        if (e.getErrorCode() != StreamErrorCode.CANCELLED) channel.sendResponse(e);
    } catch (Exception e) {
        channel.sendResponse(e);
    }
}
```

### Client-side handler

Implement `StreamTransportResponseHandler` and read typed vectors from the response:

```java
class MyQueryHandler implements StreamTransportResponseHandler<MyQueryResponse> {

    public MyQueryResponse read(StreamInput in) throws IOException {
        return new MyQueryResponse(in);
    }

    public void handleStreamResponse(StreamTransportResponse<MyQueryResponse> stream) {
        MyQueryResponse response;
        while ((response = stream.nextResponse()) != null) {
            VectorSchemaRoot root = response.getRoot();
            VarCharVector names = (VarCharVector) root.getVector("name");
            Float8Vector scores = (Float8Vector) root.getVector("score");
            // process typed vectors...
        }
        stream.close();
    }

    // ...
}
```

## Allocator management

All allocators must share the same root allocator for zero-copy transfer to work. Use the channel's allocator obtained via `ArrowFlightChannel.from(channel).getAllocator()` to create `VectorSchemaRoot` instances. The framework transfers buffer ownership from the producer's root into the Flight stream's shared root — no data copying.

Batches can be produced in parallel (e.g., by worker threads) as long as each batch creates its own `VectorSchemaRoot` from the channel's allocator. The framework serializes the transfer and send on the executor thread.

## Existing byte-serialized path

The existing byte serialization path (`writeTo(StreamOutput)` / `read(StreamInput)`) is unchanged. Responses that extend `ActionResponse` directly continue to use byte serialization through `VarBinaryVector`. Both paths coexist.
