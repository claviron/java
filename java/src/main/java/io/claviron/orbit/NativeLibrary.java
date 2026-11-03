package io.claviron.orbit;

import java.nio.file.Path;

/** Explicit owner of a version-checked native library. JDK 25 native access is required. */
public final class NativeLibrary implements AutoCloseable {
    final Ffm ffi;
    private int runtimes;
    private boolean closed;

    private NativeLibrary(Path path) {
        ffi = new Ffm(path);
    }

    public static NativeLibrary load(Path path) {
        return new NativeLibrary(path);
    }

    /** All budgets are explicit. No common pool or default completion executor is used. */
    public synchronized NativeRuntime openRuntime(int maxOperations, long maxBufferBytes, int completionThreads) {
        if (closed) throw new IllegalStateException("Native library is closed");
        if (maxOperations <= 0 || maxBufferBytes <= 0 || completionThreads <= 0) {
            throw new IllegalArgumentException("Operation, byte and completion-thread budgets must be positive");
        }
        NativeRuntime runtime = new NativeRuntime(this, maxOperations, completionThreads);
        long handle = ffi.open(maxOperations, maxBufferBytes);
        runtimes++;
        try {
            runtime.initialize(handle);
            return runtime;
        } catch (RuntimeException | Error failure) {
            try {
                runtime.close();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    synchronized void detach() {
        runtimes--;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        if (runtimes != 0) throw new NativeException(5);
        ffi.arena.close();
        closed = true;
    }
}
