package dev.claviron.orbit;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** One accepted invocation; not an HTTP exchange. Manual consumers must reply and close it. */
public final class OrbitInvocation implements AutoCloseable {
    private final OrbitInvocations owner;
    private final long handle;
    private final InvokeFfm.Metadata metadata;
    private final CompletableFuture<Void> cancelled = new CompletableFuture<>();
    private final CompletionStage<NativeBuffer> terminal;
    private boolean closed;
    OrbitInvocation(OrbitInvocations owner, long handle) {
        this.owner = owner; this.handle = handle;
        metadata = owner.api.info(handle);
        NativeOperation watch = owner.application.runtime.submit(ignored -> owner.api.operation(owner.api.watch, handle));
        terminal = watch.completion();
        terminal.whenComplete((ignored, failure) -> {
            if (failure != null && OrbitRequest.unwrap(failure) instanceof CancellationException) cancelled.complete(null);
        });
    }
    public String operation() { return metadata.operation(); }
    public ByteBuffer payload() { return ByteBuffer.wrap(metadata.payload()).asReadOnlyBuffer(); }
    public long generation() { return metadata.generation(); }
    /** Local generation retirement/abandonment, not proof that a remote caller still waits. */
    public CompletionStage<Void> cancelled() { return cancelled.minimalCompletionStage(); }
    CompletionStage<NativeBuffer> terminal() { return terminal; }
    synchronized boolean current() { return !closed && owner.api.current(handle); }
    public synchronized void reply(InvokeReply reply) {
        if (closed) throw new CancellationException("Invocation closed");
        owner.api.reply(handle, reply);
    }
    @Override public synchronized void close() {
        if (closed) return;
        Ffm.check(owner.application.runtime.library.ffi.one(owner.api.requestRelease, handle));
        closed = true;
    }
}
