package example;

import dev.claviron.orbit.*;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;

/** Deterministic cross-process fixtures, never packaged in the production JAR. */
public final class StreamingApplication {
    private StreamingApplication() {}
    private static void signal(String value) { System.out.println(value); System.out.flush(); }
    private static void check(boolean condition, String reason) {
        if (!condition) throw new AssertionError(reason);
    }
    private static void status(int expected, Runnable call) {
        try { call.run(); throw new AssertionError("Expected native rejection " + expected); }
        catch (NativeException error) { check(error.status() == expected, "Unexpected native status: " + error); }
    }
    private static void echo(OrbitRequest request, OrbitResponseWriter writer) {
        for (;;) {
            NativeBuffer chunk = request.readBody(4096).completion().toCompletableFuture().join();
            if (chunk == null) {
                check(request.readBody(1).completion().toCompletableFuture().join() == null, "EOF changed on reread");
                return;
            }
            try (chunk) {
                writer.write(chunk.view().toArray(ValueLayout.JAVA_BYTE)).toCompletableFuture().join();
            }
        }
    }

    @SuppressWarnings("try") // Early close tests drain; try-with-resources still guarantees failure cleanup.
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("usage: StreamingApplication <native-library> <fleet> <bytes-larger-than-Link-ring>");
        int transfer = Integer.parseInt(args[2]);
        try (NativeLibrary library = NativeLibrary.load(Path.of(args[0]));
             NativeRuntime runtime = library.openRuntime(64, Math.multiplyExact(transfer, 4L), 2);
             var executor = Executors.newVirtualThreadPerTaskExecutor();
             OrbitApplication application = runtime.join(new ApplicationOptions(args[1], "stream", 1, 8, 16, 16))) {
            application.serve(request -> {
                String path = request.uri().getRawPath();
                if (path.equals("/plain")) return CompletableFuture.completedStage(OrbitResponse.text("healthy"));
                return CompletableFuture.completedStage(OrbitResponse.streaming(200,
                        List.of(Header.text("content-type", "application/octet-stream")), writer -> {
                    status(11, () -> request.beginResponse(201, List.of()));
                    if (path.equals("/echo") || path.equals("/drain")) {
                        return CompletableFuture.runAsync(() -> {
                            if (path.equals("/drain")) signal("DRAIN_PENDING");
                            echo(request, writer);
                        }, executor);
                    }
                    if (path.equals("/read-cancel") || path.equals("/read-abort")) {
                        status(4, () -> request.readBody(Math.multiplyExact(transfer, 5)));
                        NativeOperation read = request.readBody(4096);
                        status(5, () -> request.readBody(4096));
                        if (path.equals("/read-abort")) check(read.cancel(), "Read cancellation lost while no bytes were supplied");
                        else signal("READ_PENDING");
                        return read.completion().handle((value, failure) -> {
                            check(failure != null && value == null, "Cancelled read returned bytes/EOF");
                            return null;
                        }).thenCompose(ignored -> request.cancelled()).thenRun(() -> signal(
                                path.equals("/read-abort") ? "READ_ABORTED" : "READ_CANCELLED"));
                    }
                    if (path.equals("/producer-fail")) {
                        return writer.write(new byte[]{1, 2, 3}).thenCompose(ignored ->
                                CompletableFuture.failedStage(new IllegalStateException("Producer failed after head")));
                    }
                    status(4, () -> writer.write(new byte[Math.multiplyExact(transfer, 5)]));
                    byte[] bytes = new byte[transfer];
                    Arrays.fill(bytes, (byte) 42);
                    CompletionStage<Void> write = writer.write(bytes);
                    Arrays.fill(bytes, (byte) 99); // The native downcall already owns the submitted bytes.
                    check(!write.toCompletableFuture().isDone(), "Large write completed without a reader");
                    status(5, () -> writer.write(new byte[]{1}));
                    signal(path.equals("/write-cancel") ? "WRITE_CANCEL_PENDING" : "WRITE_BLOCKED");
                    if (path.equals("/write-cancel")) {
                        return write.handle((ignored, failure) -> {
                            check(failure != null, "Cancelled backpressured write succeeded"); return null;
                        }).thenCompose(ignored -> request.cancelled()).thenRun(() -> signal("WRITE_CANCELLED"));
                    }
                    return write;
                }));
            }, executor);
            signal("STREAM_READY");
            var input = new java.io.BufferedReader(new java.io.InputStreamReader(System.in, java.nio.charset.StandardCharsets.UTF_8));
            for (String line; (line = input.readLine()) != null && !line.equals("stop"); ) {
                if (!line.equals("withdraw")) throw new IllegalArgumentException("Unknown fixture control");
                application.withdraw();
                signal("WITHDRAWN");
                // Close runs while the accepted producer is still waiting for upload bytes.
                application.close();
                signal("APPLICATION_CLOSED");
            }
        }
        signal("STREAM_DRAINED");
    }
}
