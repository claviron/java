package dev.claviron.orbit;

import java.util.concurrent.CompletionStage;

/** One pending write, no chunk queue. Await each write before reusing the writer. */
public final class OrbitResponseWriter {
    private final OrbitRequest request;
    private CompletionStage<Void> finishing;

    OrbitResponseWriter(OrbitRequest request) { this.request = request; }

    /** Copies the supplied bytes before returning; completes after Link accepts every byte. */
    public synchronized CompletionStage<Void> write(byte[] bytes) {
        if (finishing != null) throw new IllegalStateException("Response is finishing");
        return request.write(bytes);
    }

    /** Send FIN after accepted writes. Producer-based responses call this automatically. */
    public synchronized CompletionStage<Void> finish() {
        if (finishing == null) finishing = request.finishResponse();
        return finishing;
    }

    /** Abandon this exchange, reset its output and wake pending reads/writes. */
    public void abort() { request.close(); }
    public CompletionStage<Void> cancelled() { return request.cancelled(); }
}
