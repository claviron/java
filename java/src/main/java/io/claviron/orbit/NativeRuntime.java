package io.claviron.orbit;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.function.LongUnaryOperator;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/** Bounded native ownership/completions. No production Link producer is attached yet. */
public final class NativeRuntime implements AutoCloseable {
    private static final Runnable STOP = () -> {};
    final NativeLibrary library;
    private final ArrayBlockingQueue<Runnable> dispatch;
    private final Map<Long, NativeOperation> operations = new HashMap<>();
    private final List<Thread> workers = new ArrayList<>();
    private final Thread waiter;
    private final Object closeLock = new Object();
    private long handle;
    private int applications;
    private boolean shutdown;
    private boolean joined;
    private boolean released;
    private volatile Throwable driverFailure;

    NativeRuntime(NativeLibrary library, int maxOperations, int completionThreads) {
        this.library = library;
        dispatch = new ArrayBlockingQueue<>(maxOperations);
        for (int index = 0; index < completionThreads; index++) {
            workers.add(Thread.ofPlatform().name("orbit-completion-" + index).unstarted(this::dispatch));
        }
        waiter = Thread.ofPlatform().name("orbit-native-waiter").unstarted(this::receive);
    }

    void initialize(long handle) {
        this.handle = handle;
        for (Thread worker : workers) worker.start();
        waiter.start();
    }

    /** Internal producer boundary; intake adapters must register before delivery can look up the result. */
    synchronized NativeOperation submit(LongUnaryOperator producer) {
        if (shutdown) throw new NativeException(6);
        long operation = producer.applyAsLong(handle);
        NativeOperation owner = new NativeOperation(this, operation);
        if (operations.putIfAbsent(operation, owner) != null) {
            throw new IllegalStateException("Duplicate native operation identity");
        }
        return owner;
    }

    synchronized boolean cancel(NativeOperation operation) {
        if (!operations.containsKey(operation.handle)) return false;
        int status = library.ffi.one(library.ffi.cancel, operation.handle);
        if (status == Ffm.ALREADY_COMPLETED) return false;
        Ffm.check(status);
        return true;
    }

    synchronized void releaseBuffer(long buffer) {
        if (released) throw new IllegalStateException("Buffer outlived its native engine");
        Ffm.check(library.ffi.one(library.ffi.releaseBuffer, buffer));
    }

    /** Cancel pending native operations and wake the waiter; queued successful results remain owned. */
    public synchronized void shutdown() {
        if (shutdown) return;
        if (applications != 0) throw new NativeException(5);
        Ffm.check(library.ffi.one(library.ffi.shutdown, handle));
        shutdown = true;
    }

    public OrbitApplication join(ApplicationOptions options) {
        return new OrbitApplication(this, options);
    }

    void requireExternalCloser() {
        if (Thread.currentThread() == waiter || workers.contains(Thread.currentThread())) {
            throw new IllegalStateException("Cannot join native delivery from its own completion callback");
        }
    }

    synchronized long attachApplication(java.util.function.LongUnaryOperator join) {
        if (shutdown) throw new NativeException(6);
        long application = join.applyAsLong(handle);
        applications++;
        return application;
    }

    synchronized void detachApplication() { applications--; }

    private void receive() {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment record = scratch.allocate(Ffm.COMPLETION);
            for (;;) {
                int status = library.ffi.wait(handle, record);
                if (status == Ffm.END) return;
                Ffm.check(status);
                long operation = record.get(JAVA_LONG, 0);
                int result = record.get(JAVA_INT, 8);
                long buffer = record.get(JAVA_LONG, 16);
                long resource = record.get(JAVA_LONG, 24);
                NativeOperation owner;
                synchronized (this) {
                    owner = operations.get(operation);
                    if (owner != null) { owner.resource = resource; owner.enqueued = true; }
                }
                if (owner == null) throw new IllegalStateException("Completion has no registered owner");
                // Credits remain held while queued; a worker retires them before user callbacks.
                if (!dispatch.offer(() -> deliver(owner, result, buffer))) {
                    throw new IllegalStateException("Reserved completion capacity was exceeded");
                }
            }
        } catch (Throwable failure) {
            driverFailure = failure;
            List<NativeOperation> pending;
            synchronized (this) {
                shutdown = true;
                pending = operations.values().stream().filter(op -> !op.enqueued).toList();
                for (NativeOperation operation : pending) operation.enqueued = true;
            }
            // A broken ABI/driver must never run user callbacks on this waiter.
            for (NativeOperation operation : pending) {
                if (!dispatch.offer(() -> operation.completion.completeExceptionally(failure))) {
                    driverFailure.addSuppressed(new IllegalStateException("Reserved failure delivery capacity was exceeded"));
                }
            }
        }
    }

    private void deliver(NativeOperation owner, int status, long buffer) {
        NativeBuffer lease = null;
        Throwable failure = null;
        try {
            if (status == 0 && buffer != 0) lease = new NativeBuffer(this, buffer);
            else if (status == 0 || status == Ffm.END) { /* successful no-payload/EOF completion */ }
            else if (status == 7) failure = new CancellationException("Native operation cancelled");
            else failure = new NativeException(status);
        } catch (Throwable error) {
            failure = error;
        }
        try {
            synchronized (this) {
                Ffm.check(library.ffi.one(library.ffi.releaseOperation, owner.handle));
                operations.remove(owner.handle);
            }
        } catch (Throwable error) {
            driverFailure = error;
            if (lease != null) lease.close();
            failure = error;
        }
        // Retire native credit before invoking callbacks, including chained submissions.
        if (failure != null) owner.completion.completeExceptionally(failure);
        else if (!owner.completion.complete(lease) && lease != null) lease.close();
    }

    private void dispatch() {
        for (;;) {
            Runnable task;
            try {
                task = dispatch.take();
            } catch (InterruptedException interrupted) {
                // A user callback may interrupt its completion thread. No work is dropped.
                continue;
            }
            if (task == STOP) return;
            task.run();
        }
    }

    private static void join(Thread thread) {
        boolean interrupted = false;
        for (;;) {
            try {
                thread.join();
                break;
            } catch (InterruptedException signal) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private void stopWorker() {
        boolean interrupted = false;
        for (;;) {
            try {
                dispatch.put(STOP);
                break;
            } catch (InterruptedException signal) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    @Override
    public void close() {
        if (Thread.currentThread() == waiter || workers.contains(Thread.currentThread())) {
            throw new IllegalStateException("A runtime cannot synchronously join its own completion thread");
        }
        synchronized (closeLock) {
            if (released) return;
            shutdown();
            if (!joined) {
                join(waiter);
                // Any worker can consume a stop marker. Snapshot before sending
                // markers, or one worker exiting can hide another worker's stop.
                long alive = workers.stream().filter(Thread::isAlive).count();
                for (long index = 0; index < alive; index++) stopWorker();
                for (Thread worker : workers) join(worker);
                joined = true;
            }
            if (driverFailure != null) {
                throw new IllegalStateException("Native driver failed; library remains pinned", driverFailure);
            }
            synchronized (this) {
                Ffm.check(library.ffi.one(library.ffi.releaseEngine, handle));
                released = true;
            }
            library.detach();
        }
    }
}
