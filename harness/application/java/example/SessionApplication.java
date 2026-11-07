package example;

import dev.claviron.orbit.*;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;

/** Deterministic message-session fixture, never packaged in the production JAR. */
public final class SessionApplication {
    private SessionApplication() {}
    private static final int LIMIT = 16;
    private static void signal(String value) { System.out.println(value); System.out.flush(); }

    private static CompletionStage<Void> serve(OrbitSessionRequest offer) throws Exception {
        String path = offer.request().uri().getRawPath();
        switch (path) {
            case "/session-reject": {
                String token = offer.request().headers().stream()
                        .filter(header -> header.name().equalsIgnoreCase("x-token")).findFirst().orElseThrow().latin1();
                return offer.reject(403, List.of(Header.text("x-refused", token)));
            }
            case "/session-silent": return CompletableFuture.completedStage(null);
            case "/session-failed": throw new IllegalStateException("session handler failed");
            case "/session-end": return offer.accept(LIMIT, List.of()).thenApply(session -> null);
            case "/session-close":
                return offer.accept(LIMIT, List.of()).thenCompose(session -> session.close(4000, "bye"));
            default:
                return CompletableFuture.runAsync(() -> echo(offer));
        }
    }

    private static void echo(OrbitSessionRequest offer) {
        OrbitSession session = offer.accept(LIMIT, List.of(Header.text("x-accepted", offer.kind().name())))
                .toCompletableFuture().join();
        try { session.sendBinary(new byte[LIMIT + 1]); throw new AssertionError("Oversized send was accepted"); }
        catch (NativeException error) { if (error.status() != 4) throw error; }
        for (;;) {
            OrbitMessage message;
            try { message = session.receive().toCompletableFuture().join(); }
            catch (java.util.concurrent.CompletionException error) {
                if (error.getCause() instanceof NativeException failure && failure.status() == 4) {
                    signal("SESSION_TOO_LARGE");
                    return;
                }
                throw error;
            }
            if (message == null) { signal("SESSION_END"); return; }
            switch (message.kind()) {
                case TEXT -> session.sendText(message.text()).toCompletableFuture().join();
                case BINARY -> session.sendBinary(message.bytes()).toCompletableFuture().join();
                case CLOSE -> {
                    signal("SESSION_CLOSE " + message.closeCode() + " " + message.text());
                    if (session.receive().toCompletableFuture().join() != null) throw new AssertionError("Message after close");
                    return;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("usage: SessionApplication <native-library> <fleet>");
        try (NativeLibrary library = NativeLibrary.load(Path.of(args[0]));
             NativeRuntime runtime = library.openRuntime(64, 1 << 20, 2);
             var executor = Executors.newVirtualThreadPerTaskExecutor();
             OrbitApplication application = runtime.join(new ApplicationOptions(args[1], "app", 1, 8, 16, 16))) {
            application.serve(request -> CompletableFuture.completedStage(OrbitResponse.text("http")), SessionApplication::serve, executor);
            signal("SESSION_READY");
            if (System.in.read() < 0) throw new IllegalStateException("Stop command missing");
            application.withdraw();
        }
        signal("SESSION_STOPPED");
    }
}
