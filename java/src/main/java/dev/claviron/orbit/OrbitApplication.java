package dev.claviron.orbit;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** One application lane in this JVM, using the existing Claviron application transport. */
public final class OrbitApplication implements AutoCloseable {
    @FunctionalInterface
    public interface Handler {
        CompletionStage<OrbitResponse> handle(OrbitRequest request) throws Exception;
    }

    final NativeRuntime runtime;
    final ApplicationFfm api;
    private final long handle;
    private final Object calls = new Object();
    private final Object closeLock = new Object();
    private final Object pending = new Object();
    private final AtomicBoolean withdrawn = new AtomicBoolean();
    private final CompletableFuture<Void> serving = new CompletableFuture<>();
    private final CompletableFuture<Void> eventServing = new CompletableFuture<>();
    private final CompletableFuture<Void> controlServing = new CompletableFuture<>();
    private final Thread controlReader;
    private record ControlListener(Consumer<OrbitControl> listener, Executor executor) {}
    private volatile ControlListener controlListener;
    private ApplicationGeneration generation;
    private final java.util.List<OrbitInvocations> invokes = new java.util.ArrayList<>();
    private Thread intake;
    private Thread eventReader;
    private int active;
    private volatile boolean released;

    OrbitApplication(NativeRuntime runtime, ApplicationOptions options) {
        this.runtime = runtime;
        api = new ApplicationFfm(runtime.library.ffi);
        handle = runtime.attachApplication(engine -> api.join(engine, options));
        controlReader = Thread.ofPlatform().name("orbit-java-controls").start(this::readControls);
    }

    /** Open an isolated invoke Link; the application owns its generation and drain. */
    public OrbitInvocations invocations(InvokeOptions options) {
        synchronized (calls) {
            requireLive();
            if (withdrawn.get()) throw new NativeException(6);
            OrbitInvocations binding = new OrbitInvocations(this, handle, options);
            invokes.add(binding);
            return binding;
        }
    }
    void detachInvocations(OrbitInvocations binding) { synchronized (calls) { invokes.remove(binding); } }

    public boolean running() {
        synchronized (calls) { requireLive(); return api.state(handle).running(); }
    }
    /** Captures a stable identity/signal; a later start/restart supplies a new object. */
    public ApplicationGeneration generation() {
        synchronized (calls) {
            requireLive();
            long id = api.state(handle).generation();
            if (generation == null || generation.id() != id) {
                NativeOperation operation = runtime.submit(ignored -> api.watchGeneration(handle, id));
                generation = new ApplicationGeneration(id, operation.completion().thenApply(ignored -> null));
            }
            return generation;
        }
    }
    public void start() { action(1); }
    public void stop() { action(2); }
    public void restart() { action(3); }
    private void action(int value) { synchronized (calls) { requireLive(); api.action(handle, value); } }
    private void requireLive() { if (released) throw new NativeException(6); }

    /** Register one asynchronous callback consumer. Native transitions never depend on callback progress. */
    public CompletionStage<Void> controls(Consumer<OrbitControl> listener, Executor executor) {
        synchronized (calls) {
            requireLive();
            if (controlListener != null) throw new IllegalStateException("Control listener already registered");
            controlListener = new ControlListener(java.util.Objects.requireNonNull(listener), java.util.Objects.requireNonNull(executor));
        }
        return controlServing.minimalCompletionStage();
    }
    private void readControls() {
        try {
            for (;;) {
                NativeOperation operation;
                synchronized (calls) {
                    if (withdrawn.get()) break;
                    operation = runtime.submit(ignored -> api.operation(api.controlNext, handle));
                }
                operation.completion().toCompletableFuture().join();
                if (operation.resource == 0) break;
                try (OrbitControl control = api.control(operation.resource)) {
                    ControlListener config = controlListener;
                    if (config != null) CompletableFuture.runAsync(() -> config.listener().accept(control), config.executor()).join();
                }
            }
            controlServing.complete(null);
        } catch (Throwable failure) {
            controlServing.completeExceptionally(OrbitRequest.unwrap(failure));
            withdraw();
        }
    }

    public CompletionStage<OrbitRequest> nextRequest() {
        synchronized (calls) {
            if (withdrawn.get()) return CompletableFuture.completedStage(null);
            NativeOperation operation = runtime.submit(ignored -> api.operation(api.next, handle));
            return operation.completion().thenApply(ignored -> {
                if (operation.resource == 0) return null;
                try { return new OrbitRequest(this, operation.resource); }
                catch (Throwable failure) {
                    Ffm.check(runtime.library.ffi.one(api.requestRelease, operation.resource));
                    throw failure;
                }
            });
        }
    }

    public CompletionStage<OrbitEvent> nextEvent() {
        synchronized (calls) {
            if (withdrawn.get()) return CompletableFuture.completedStage(null);
            NativeOperation operation = runtime.submit(ignored -> api.operation(api.eventNext, handle));
            return operation.completion().thenApply(ignored -> operation.resource == 0 ? null : api.event(operation.resource));
        }
    }

    public void subscribe(String topic) { interest(topic, true); }
    public void unsubscribe(String topic) { interest(topic, false); }

    private void interest(String topic, boolean enabled) {
        synchronized (calls) {
            if (withdrawn.get()) throw new NativeException(6);
            api.subscribe(handle, topic, enabled);
        }
    }

    /** Publish an ApplicationEvent once into this application's own fleet ring. */
    public void publish(String topic, String json) {
        synchronized (calls) {
            if (released) throw new NativeException(6);
            api.publish(handle, topic, json);
        }
    }

    /** The caller supplies an asynchronous handler executor; no framework/common pool is selected. */
    public CompletionStage<Void> serve(Handler handler, Executor executor) {
        synchronized (calls) {
            if (intake != null || withdrawn.get()) throw new IllegalStateException("Application intake already started or closed");
            intake = Thread.ofPlatform().name("orbit-java-intake").start(() -> {
                Throwable failure = null;
                try {
                    for (;;) {
                        OrbitRequest request = nextRequest().toCompletableFuture().join();
                        if (request == null) break;
                        synchronized (pending) { active++; }
                        try { executor.execute(() -> handle(request, handler)); }
                        catch (Throwable error) { finish(request); }
                    }
                } catch (Throwable error) {
                    failure = OrbitRequest.unwrap(error);
                    withdraw();
                } finally {
                    boolean interrupted = false;
                    synchronized (pending) {
                        while (active != 0) {
                            try { pending.wait(); }
                            catch (InterruptedException signal) { interrupted = true; }
                        }
                    }
                    if (interrupted) Thread.currentThread().interrupt();
                    if (failure == null) serving.complete(null); else serving.completeExceptionally(failure);
                }
            });
        }
        return serving.minimalCompletionStage();
    }

    private void handle(OrbitRequest request, Handler handler) {
        // Retire bridge work even if Java ignores cancellation and returns late.
        // The JVM/executor owns that computation; no thread is forcibly stopped.
        AtomicBoolean retired = new AtomicBoolean();
        Runnable retire = () -> { if (retired.compareAndSet(false, true)) finish(request); };
        request.cancelled().whenComplete((ignored, failure) -> retire.run());
        if (retired.get() || !request.current()) { retire.run(); return; }
        CompletionStage<OrbitResponse> response;
        try {
            response = handler.handle(request);
            if (response == null) throw new IllegalStateException("Handler returned no response stage");
        } catch (Throwable failure) { response = CompletableFuture.failedStage(failure); }
        response.whenComplete((value, failure) -> {
            if (retired.get()) return;
            OrbitResponse result = failure == null && value != null ? value : OrbitResponse.text(500,
                    java.util.List.of(Header.text("content-type", "text/plain")), "Internal Server Error");
            try { request.respond(result).whenComplete((ignored, error) -> retire.run()); }
            catch (Throwable error) { retire.run(); }
        });
    }

    private void finish(OrbitRequest request) {
        try { request.close(); }
        finally {
            request.terminal().whenComplete((ignored, failure) -> {
                synchronized (pending) { active--; pending.notifyAll(); }
            });
        }
    }

    /** Sequential event delivery. A slow subscriber backpressures only its bounded event path. */
    public CompletionStage<Void> events(Consumer<OrbitEvent> listener, Executor executor) {
        synchronized (calls) {
            if (eventReader != null || withdrawn.get()) throw new IllegalStateException("Event reader already started or closed");
            eventReader = Thread.ofPlatform().name("orbit-java-events").start(() -> {
                try {
                    for (;;) {
                        OrbitEvent event = nextEvent().toCompletableFuture().join();
                        if (event == null) break;
                        try (event) { CompletableFuture.runAsync(() -> listener.accept(event), executor).join(); }
                    }
                    eventServing.complete(null);
                } catch (Throwable failure) { eventServing.completeExceptionally(OrbitRequest.unwrap(failure)); }
            });
        }
        return eventServing.minimalCompletionStage();
    }

    /** Safe from handlers: stop intake now; close outside a handler to join its work. */
    public void withdraw() {
        synchronized (calls) {
            if (!withdrawn.get()) {
                Ffm.check(runtime.library.ffi.one(api.withdraw, handle));
                withdrawn.set(true);
            }
        }
    }

    @Override
    public void close() {
        runtime.requireExternalCloser();
        if (Thread.currentThread() == intake || Thread.currentThread() == eventReader || Thread.currentThread() == controlReader) {
            throw new IllegalStateException("Application cannot join its own delivery thread");
        }
        synchronized (closeLock) {
            if (released) return;
            withdraw();
            join(intake);
            join(eventReader);
            join(controlReader);
            java.util.List<OrbitInvocations> children;
            synchronized (calls) { children = java.util.List.copyOf(invokes); }
            Throwable childFailure = null;
            for (OrbitInvocations binding : children) {
                try { binding.close(); }
                catch (Throwable error) { if (childFailure == null) childFailure = error; else childFailure.addSuppressed(error); }
            }
            NativeOperation operation = runtime.submit(ignored -> api.operation(api.closed, handle));
            Throwable failure = null;
            try { operation.completion().toCompletableFuture().join(); }
            catch (Throwable error) { failure = OrbitRequest.unwrap(error); }
            synchronized (calls) {
                Ffm.check(runtime.library.ffi.one(api.release, handle));
                released = true;
                runtime.detachApplication();
            }
            if (failure != null) throw Ffm.unexpected(failure);
            if (childFailure != null) throw Ffm.unexpected(childFailure);
            if (intake != null) serving.join();
            if (eventReader != null) eventServing.join();
            controlServing.join();
        }
    }

    private static void join(Thread thread) {
        if (thread == null) return;
        boolean interrupted = false;
        for (;;) {
            try { thread.join(); break; }
            catch (InterruptedException signal) { interrupted = true; }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }
}
