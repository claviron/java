package io.claviron.orbit;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/** A scoped immutable native lease. Close invalidates previously obtained Java views. */
public final class NativeBuffer implements AutoCloseable {
    private final NativeRuntime runtime;
    private final long handle;
    private final Arena lifetime = Arena.ofShared();
    private final MemorySegment segment;
    private boolean closed;

    NativeBuffer(NativeRuntime runtime, long handle) {
        this.runtime = runtime;
        this.handle = handle;
        try {
            segment = runtime.library.ffi.view(handle, lifetime);
        } catch (Throwable failure) {
            lifetime.close();
            runtime.releaseBuffer(handle);
            throw failure;
        }
    }

    public synchronized MemorySegment view() {
        if (closed) throw new IllegalStateException("Native buffer is closed");
        return segment;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        lifetime.close();
        runtime.releaseBuffer(handle);
        closed = true;
    }
}
