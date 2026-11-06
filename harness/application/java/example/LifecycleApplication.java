package example;

import dev.claviron.orbit.*;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executors;

/** Lifecycle acceptance fixtures, outside the production JAR. */
public final class LifecycleApplication {
    private LifecycleApplication() {}
    private static void check(boolean value, String reason) { if (!value) throw new AssertionError(reason); }
    private static synchronized void signal(String value) { System.out.println(value); System.out.flush(); }
    public static void main(String[] args) throws Exception {
        if (args.length != 5) throw new IllegalArgumentException("usage: LifecycleApplication <library> <fleet> <name> <tag> <transfer-bytes>");
        String tag = args[3];
        AtomicLong calls = new AtomicLong();
        AtomicLong stoppedCalls = new AtomicLong(-1);
        AtomicLong controlCount = new AtomicLong();
        AtomicLong lateProducers = new AtomicLong();
        AtomicReference<CompletableFuture<OrbitResponse>> late = new AtomicReference<>();
        int transfer = Integer.parseInt(args[4]);
        try (NativeLibrary library = NativeLibrary.load(Path.of(args[0]));
             NativeRuntime runtime = library.openRuntime(96, transfer * 4L, 2);
             var executor = Executors.newVirtualThreadPerTaskExecutor();
             OrbitApplication application = runtime.join(new ApplicationOptions(args[1], args[2], 17, 16, 1, 16))) {
            CompletableFuture<Void> releaseEvents = new CompletableFuture<>();
            if (!tag.equals("other")) {
                var firstEvent = new java.util.concurrent.atomic.AtomicBoolean();
                application.subscribe("example.busy");
                application.events(event -> {
                    if (firstEvent.compareAndSet(false, true)) signal("EVENT_BLOCKED " + tag);
                    releaseEvents.join();
                }, executor);
            }
            AtomicReference<ApplicationGeneration> captured = new AtomicReference<>(application.generation());
            application.controls(control -> {
                controlCount.incrementAndGet();
                if (control.kind() == OrbitControl.Kind.LIFECYCLE) {
                    ApplicationGeneration previous = captured.get();
                    ApplicationGeneration current = application.generation();
                    check(current.id() == control.generation(), "Generation was not published before lifecycle callback");
                    check(application.running() == control.running(), "Running state differs from callback");
                    if (control.action() == OrbitControl.Action.STOP) {
                        previous.cancelled().toCompletableFuture().join();
                        stoppedCalls.set(calls.get());
                    } else {
                        if (previous.id() != current.id()) previous.cancelled().toCompletableFuture().join();
                        else check(previous == current, "Idempotent start replaced the generation signal");
                        check(!current.cancelled().toCompletableFuture().isDone(), "New generation already cancelled");
                    }
                    captured.set(current);
                    signal("CONTROL " + tag + " " + control.action() + " " + control.generation() + " " + control.running());
                } else if (control.kind() == OrbitControl.Kind.COMMAND) {
                    check(control.name().equals("probe"), "Unexpected command name");
                    check(control.payload().remaining() == 2 && control.payload().get(0) == 0 && control.payload().get(1) == (byte)255,
                            "Opaque command payload changed");
                    signal("PROBE " + tag);
                } else {
                    check(control.edge().equals("example-edge"), "Shutdown edge changed");
                    check(application.running(), "Edge shutdown stopped the whole application");
                    signal("SHUTDOWN " + tag);
                }
            }, executor);
            application.serve(request -> {
                calls.incrementAndGet();
                switch (request.uri().getRawPath()) {
                    case "/late" -> {
                        CompletableFuture<OrbitResponse> future = new CompletableFuture<>();
                        late.set(future);
                        signal("HELD " + tag + " " + request.generation());
                        return future;
                    }
                    case "/read", "/write" -> {
                        return CompletableFuture.completedStage(OrbitResponse.streaming(200, List.of(), writer -> {
                            if (request.uri().getRawPath().equals("/read")) {
                                var read = request.readBody(4096).completion();
                                signal("READ " + tag + " " + request.generation());
                                return read.thenApply(chunk -> { if (chunk != null) chunk.close(); return null; });
                            }
                            byte[] bytes = new byte[transfer];
                            var write = writer.write(bytes);
                            check(!write.toCompletableFuture().isDone(), "Write was not backpressured");
                            signal("WRITE " + tag + " " + request.generation());
                            return write;
                        }));
                    }
                    default -> { return CompletableFuture.completedStage(OrbitResponse.text(tag + ":" + request.generation())); }
                }
            }, executor);
            signal("LIFE_READY " + tag + " " + ProcessHandle.current().pid());
            var input = new java.io.BufferedReader(new java.io.InputStreamReader(System.in, java.nio.charset.StandardCharsets.UTF_8));
            for (String line; (line = input.readLine()) != null && !line.equals("stop"); ) {
                switch (line) {
                    case "check-stopped" -> {
                        check(!application.running() && calls.get() == stoppedCalls.get(), "Stopped request invoked a handler");
                        signal("STOPPED_CHECKED " + tag);
                    }
                    case "complete-late" -> {
                        check(late.get().complete(OrbitResponse.streaming(200, List.of(), writer -> {
                            lateProducers.incrementAndGet(); return CompletableFuture.completedStage(null);
                        })), "Late future already completed");
                        check(lateProducers.get() == 0, "Retired handler's producer was invoked");
                        signal("LATE_DROPPED " + tag);
                    }
                    case "restart" -> application.restart();
                    case "block-events" -> {
                        for (int i = 0; i < 4; i++) application.publish("example.busy", "null");
                        signal("EVENTS_PUBLISHED " + tag);
                    }
                    case "release-events" -> { releaseEvents.complete(null); signal("EVENTS_RELEASED " + tag); }
                    case "check-unrelated" -> {
                        check(controlCount.get() == 0 && application.running() && application.generation().id() == 1,
                                "Lifecycle crossed application identity");
                        signal("UNRELATED_CHECKED " + tag);
                    }
                    default -> throw new IllegalArgumentException("Unknown lifecycle fixture control");
                }
            }
        }
        signal("LIFE_DRAINED " + tag);
    }
}
