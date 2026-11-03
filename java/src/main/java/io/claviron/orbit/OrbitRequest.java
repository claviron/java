package io.claviron.orbit;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Owned request metadata and explicit exchange lifetime; no framework request type. */
public final class OrbitRequest implements AutoCloseable {
    private final OrbitApplication application;
    private final long handle;
    private final ApplicationFfm.Metadata metadata;
    private final CompletableFuture<Void> cancelled = new CompletableFuture<>();
    private final CompletionStage<NativeBuffer> terminal;
    private boolean closed;

    OrbitRequest(OrbitApplication application, long handle) {
        this.application = application;
        this.handle = handle;
        metadata = application.api.request(handle);
        NativeOperation watch = application.runtime.submit(ignored -> application.api.operation(application.api.watch, handle));
        terminal = watch.completion();
        terminal.whenComplete((ignored, failure) -> {
            if (failure != null && unwrap(failure) instanceof CancellationException) cancelled.complete(null);
        });
    }

    public long generation() { return metadata.generation(); }
    synchronized boolean current() { return !closed && application.api.current(handle); }

    public String method() { return metadata.method(); }
    public URI uri() { return metadata.uri(); }
    public String httpVersion() { return metadata.version(); }
    public List<Header> headers() { return metadata.headers(); }
    public CompletionStage<Void> cancelled() { return cancelled.minimalCompletionStage(); }
    CompletionStage<NativeBuffer> terminal() { return terminal; }

    /**
     * One pending read per exchange. Null completion is EOF; close every returned lease.
     * The requested capacity stays reserved until lease release. Cancelling the operation
     * abandons this exchange because a racing read may already have consumed bytes.
     */
    public synchronized NativeOperation readBody(int maxBytes) {
        requireOpen();
        return application.runtime.submit(ignored -> application.api.read(handle, maxBytes));
    }

    /** Commit a streaming head once. The manual caller owns writes, finish and request close. */
    public synchronized CompletionStage<OrbitResponseWriter> beginResponse(int status, List<Header> headers) {
        requireOpen();
        NativeOperation operation = application.runtime.submit(ignored -> application.api.head(handle, status, headers));
        return operation.completion().thenApply(ignored -> new OrbitResponseWriter(this));
    }

    public CompletionStage<Void> respond(OrbitResponse response) {
        if (!response.streaming()) {
            NativeOperation operation;
            synchronized (this) {
                requireOpen();
                operation = application.runtime.submit(ignored -> application.api.respond(handle, response));
            }
            return operation.completion().thenApply(ignored -> null);
        }
        return beginResponse(response.status(), response.headers()).thenCompose(writer -> {
            CompletionStage<Void> produced;
            try {
                produced = response.producer().produce(writer);
                if (produced == null) throw new IllegalStateException("Producer returned no completion stage");
            } catch (Throwable failure) { produced = CompletableFuture.failedStage(failure); }
            return produced.thenCompose(ignored -> writer.finish()).whenComplete((ignored, failure) -> {
                if (failure != null) writer.abort();
            });
        });
    }

    synchronized CompletionStage<Void> write(byte[] bytes) {
        requireOpen();
        NativeOperation operation = application.runtime.submit(ignored -> application.api.write(handle, bytes));
        return operation.completion().thenApply(ignored -> null);
    }

    synchronized CompletionStage<Void> finishResponse() {
        requireOpen();
        NativeOperation operation = application.runtime.submit(ignored -> application.api.operation(application.api.finish, handle));
        return operation.completion().thenApply(ignored -> null);
    }

    private void requireOpen() {
        if (closed) throw new CancellationException("Request closed");
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        Ffm.check(application.runtime.library.ffi.one(application.api.requestRelease, handle));
        closed = true;
    }

    static Throwable unwrap(Throwable value) {
        while (value.getCause() != null && (value instanceof java.util.concurrent.CompletionException
                || value instanceof java.util.concurrent.ExecutionException)) value = value.getCause();
        return value;
    }
}
