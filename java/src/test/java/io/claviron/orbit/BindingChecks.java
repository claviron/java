package io.claviron.orbit;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;

/** Deterministic FFM acceptance checks without a server, framework or test dependency. */
public final class BindingChecks {
    private BindingChecks() {}

    public static void main(String[] args) throws Exception {
        try (NativeLibrary production = NativeLibrary.load(Path.of(args[0]))) {
            check(production.ffi.symbols.find("oj_fixture_submit").isEmpty(), "fixture leaked into production");
            try (NativeRuntime unused = production.openRuntime(1, 8, 1)) {
                expectStatus(5, production::close);
                unused.shutdown();
            }
            expect(IllegalArgumentException.class, () -> production.openRuntime(0, 8, 1));
        }
        try (NativeLibrary library = NativeLibrary.load(Path.of(args[1]))) {
            Fixture fixture = new Fixture(library);
            List<Runnable> checks = List.of(
                    () -> abiMismatch(library),
                    () -> earlyCompletion(library, fixture),
                    () -> ownedLease(library, fixture),
                    () -> separateBudgets(library, fixture),
                    () -> saturatedShutdown(library, fixture),
                    () -> blockedCallback(library, fixture),
                    () -> chainedSubmission(library, fixture),
                    () -> emptyLease(library, fixture));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> runs = checks.stream().map(executor::submit).toList();
                for (Future<?> run : runs) run.get();
            }
        }
        System.out.println("Java FFM: production isolation and 8 parallel ownership/completion checks passed");
    }

    private static void abiMismatch(NativeLibrary library) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment info = arena.allocate(Ffm.INFO);
            Ffm.check(library.ffi.query(info));
            info.set(JAVA_INT, 4, Ffm.ABI + 1);
            expectStatus(9, () -> Ffm.validate(info));
            Ffm.check(library.ffi.query(info));
            info.set(JAVA_INT, 28, 1);
            expectStatus(9, () -> Ffm.validate(info));
        }
    }

    private static void earlyCompletion(NativeLibrary library, Fixture fixture) {
        try (NativeRuntime runtime = library.openRuntime(1, 8, 1)) {
            NativeOperation operation = fixture.submitReady(runtime, new byte[]{9});
            try (NativeBuffer buffer = operation.completion().toCompletableFuture().join()) {
                check(buffer.view().get(JAVA_BYTE, 0) == 9, "early completion lost its Java registration");
            }
        }
    }

    private static void ownedLease(NativeLibrary library, Fixture fixture) {
        NativeRuntime runtime = library.openRuntime(2, 32, 2);
        byte[] source = {0, (byte) 255, 42};
        NativeOperation operation = fixture.submit(runtime, source);
        check(!operation.completion().toCompletableFuture().isDone(), "submission completed before producer");
        Arrays.fill(source, (byte) 1);
        fixture.complete(operation);
        NativeBuffer buffer = operation.completion().toCompletableFuture().join();
        MemorySegment retained = buffer.view();
        check(retained.isReadOnly(), "native view must be immutable");
        check(Arrays.equals(retained.toArray(JAVA_BYTE), new byte[]{0, (byte) 255, 42}), "scratch escaped submission");
        check(!operation.cancel(), "delivered operation cancelled twice");
        expectStatus(2, () -> Ffm.check(library.ffi.one(library.ffi.cancel, operation.handle)));
        expectStatus(5, runtime::close);
        check(retained.get(JAVA_BYTE, 2) == 42, "Busy close invalidated a live lease");
        expectStatus(5, library::close);
        buffer.close();
        buffer.close();
        expect(IllegalStateException.class, () -> retained.get(JAVA_BYTE, 0));
        runtime.close();
        runtime.close();
    }

    private static void separateBudgets(NativeLibrary library, Fixture fixture) {
        try (NativeRuntime runtime = library.openRuntime(1, 2, 1)) {
            NativeOperation operation = fixture.submit(runtime, new byte[]{1, 2});
            expectStatus(4, () -> fixture.submit(runtime, new byte[0]));
            expectStatus(3, () -> Ffm.check(library.ffi.one(library.ffi.releaseBuffer, operation.handle)));
            fixture.complete(operation);
            NativeBuffer buffer = operation.completion().toCompletableFuture().join();
            expectStatus(4, () -> fixture.submit(runtime, new byte[]{3}));
            buffer.close();
            NativeOperation next = fixture.submit(runtime, new byte[]{3});
            check(next.cancel(), "pending cancellation lost");
            cancelled(next);
        }
    }

    private static void saturatedShutdown(NativeLibrary library, Fixture fixture) {
        NativeRuntime runtime = library.openRuntime(2, 2, 1);
        NativeOperation first = fixture.submit(runtime, new byte[]{1});
        NativeOperation second = fixture.submit(runtime, new byte[]{2});
        runtime.shutdown();
        expectStatus(6, () -> fixture.submit(runtime, new byte[0]));
        runtime.close();
        cancelled(first);
        cancelled(second);
    }

    private static void blockedCallback(NativeLibrary library, Fixture fixture) {
        try (NativeRuntime runtime = library.openRuntime(2, 16, 2)) {
            NativeOperation first = fixture.submit(runtime, new byte[]{1});
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch resume = new CountDownLatch(1);
            CompletableFuture<Void> callback = first.completion().thenAccept(buffer -> {
                try (buffer) {
                    check(Thread.currentThread().getName().startsWith("orbit-completion-"), "callback ran on waiter");
                    expect(IllegalStateException.class, runtime::close);
                    entered.countDown();
                    await(resume);
                }
            }).toCompletableFuture();
            callback.whenComplete((ignored, failure) -> entered.countDown());
            fixture.complete(first);
            await(entered);
            try {
                NativeOperation second = fixture.submit(runtime, new byte[]{2});
                fixture.complete(second);
                try (NativeBuffer buffer = second.completion().toCompletableFuture().join()) {
                    check(buffer.view().get(JAVA_BYTE, 0) == 2, "blocked callback stalled other completions");
                }
                NativeOperation cancelled = fixture.submit(runtime, new byte[]{3});
                check(cancelled.cancel(), "cancellation could not progress");
                cancelled(cancelled);
            } finally {
                resume.countDown();
            }
            callback.join();
        }
    }

    private static void chainedSubmission(NativeLibrary library, Fixture fixture) {
        try (NativeRuntime runtime = library.openRuntime(1, 8, 1)) {
            NativeOperation first = fixture.submit(runtime, new byte[]{1});
            var chain = first.completion().thenCompose(buffer -> {
                buffer.close();
                NativeOperation second = fixture.submit(runtime, new byte[]{2});
                fixture.complete(second);
                return second.completion();
            });
            fixture.complete(first);
            try (NativeBuffer buffer = chain.toCompletableFuture().join()) {
                check(buffer.view().get(JAVA_BYTE, 0) == 2, "credit not retired before chained submission");
            }
        }
    }

    private static void emptyLease(NativeLibrary library, Fixture fixture) {
        NativeRuntime runtime = library.openRuntime(1, 1, 1);
        NativeOperation operation = fixture.submit(runtime, new byte[0]);
        fixture.complete(operation);
        NativeBuffer buffer = operation.completion().toCompletableFuture().join();
        check(buffer.view().byteSize() == 0, "empty lease was not preserved");
        expectStatus(5, runtime::close);
        buffer.close();
        runtime.close();
    }

    private static void cancelled(NativeOperation operation) {
        try {
            operation.completion().toCompletableFuture().join();
            throw new AssertionError("operation was not cancelled");
        } catch (CompletionException failure) {
            check(failure.getCause() instanceof CancellationException, "wrong cancellation cause");
        } catch (CancellationException expected) {
            // Both direct and dependent cancellation retain their distinct meaning.
        }
    }

    private static void await(CountDownLatch gate) {
        try {
            gate.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static void expectStatus(int code, Runnable operation) {
        try {
            operation.run();
            throw new AssertionError("expected native status " + code);
        } catch (NativeException failure) {
            check(failure.status() == code, "wrong native status: " + failure);
        }
    }

    private static void expect(Class<? extends Throwable> type, Runnable operation) {
        try {
            operation.run();
            throw new AssertionError("expected " + type.getName());
        } catch (Throwable failure) {
            if (!type.isInstance(failure)) throw failure;
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
