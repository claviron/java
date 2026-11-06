package dev.claviron.orbit;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** An explicitly cancellable native operation, not ownership of a Java computation. */
public final class NativeOperation {
    final NativeRuntime runtime;
    final long handle;
    boolean enqueued;
    long resource;
    final CompletableFuture<NativeBuffer> completion = new CompletableFuture<>();

    NativeOperation(NativeRuntime runtime, long handle) {
        this.runtime = runtime;
        this.handle = handle;
    }

    /** Cancelling a derived CompletableFuture does not cancel this native operation. */
    public CompletionStage<NativeBuffer> completion() {
        return completion.minimalCompletionStage();
    }

    /** False once a native terminal result has already won, even before delivery. */
    public boolean cancel() {
        return runtime.cancel(this);
    }
}
