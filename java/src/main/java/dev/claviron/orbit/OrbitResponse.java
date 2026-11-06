package dev.claviron.orbit;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** A final response with immutable bytes or a demand-driven asynchronous producer. */
public final class OrbitResponse {
    @FunctionalInterface
    public interface Producer {
        /** Complete only after all writes finish. The binding then sends FIN. */
        java.util.concurrent.CompletionStage<Void> produce(OrbitResponseWriter output) throws Exception;
    }
    private final Producer producer;
    private final int status;
    private final List<Header> headers;
    private final byte[] body;

    private OrbitResponse(int status, List<Header> headers, byte[] owned, Producer producer) {
        this.producer = producer;
        this.status = status;
        this.headers = List.copyOf(headers);
        this.body = owned;
    }

    public static OrbitResponse copyOf(int status, List<Header> headers, byte[] body) {
        return new OrbitResponse(status, headers, body.clone(), null);
    }

    public static OrbitResponse text(int status, List<Header> headers, String body) {
        return new OrbitResponse(status, headers, body.getBytes(StandardCharsets.UTF_8), null);
    }

    public static OrbitResponse text(String body) {
        return text(200, List.of(Header.text("content-type", "text/plain")), body);
    }

    public static OrbitResponse streaming(int status, List<Header> headers, Producer producer) {
        return new OrbitResponse(status, headers, null, java.util.Objects.requireNonNull(producer));
    }

    Producer producer() { return producer; }
    public boolean streaming() { return producer != null; }
    public int status() { return status; }
    public List<Header> headers() { return headers; }
    public ByteBuffer body() {
        if (streaming()) throw new IllegalStateException("Response has a streaming producer");
        return ByteBuffer.wrap(body).asReadOnlyBuffer();
    }
    byte[] bytes() { return body; }
}
