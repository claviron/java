package example;

import io.claviron.orbit.ApplicationOptions;
import io.claviron.orbit.Header;
import io.claviron.orbit.NativeLibrary;
import io.claviron.orbit.NativeBuffer;
import java.lang.foreign.ValueLayout;
import io.claviron.orbit.NativeRuntime;
import io.claviron.orbit.OrbitApplication;
import io.claviron.orbit.OrbitResponse;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/** Plain Java application; stdin closes only this process's application and resources. */
public final class PlainApplication {
    private PlainApplication() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 11) throw new IllegalArgumentException(
                "usage: PlainApplication <native-library> <fleet> <name> <instance-tag> <incarnation> "
                + "<operation-budget> <byte-budget> <completion-threads> <exchange-budget> <event-budget> <control-budget>");
        String instance = args[3];
        if (!instance.matches("[A-Za-z0-9_-]+")) throw new IllegalArgumentException("Use an alphanumeric example instance tag");
        ApplicationOptions options = new ApplicationOptions(args[1], args[2], Long.parseLong(args[4]),
                Integer.parseInt(args[8]), Integer.parseInt(args[9]), Integer.parseInt(args[10]));
        try (NativeLibrary library = NativeLibrary.load(Path.of(args[0]));
             NativeRuntime runtime = library.openRuntime(Integer.parseInt(args[5]), Long.parseLong(args[6]), Integer.parseInt(args[7]));
             var handlers = Executors.newVirtualThreadPerTaskExecutor();
             OrbitApplication application = runtime.join(options)) {
            application.subscribe("example.edge");
            application.subscribe("example.local");
            application.subscribe("example.barrier");
            application.events(event -> application.publish("example.ack", "{\"instance\":\"" + instance
                    + "\",\"event\":\"" + event.id() + "\",\"topic\":\"" + event.topic() + "\"}"), handlers);
            application.serve(request -> {
                if (request.uri().getRawPath().equals("/cancel")) {
                    return request.cancelled().thenApply(ignored -> OrbitResponse.text("cancelled"));
                }
                if (request.uri().getRawPath().equals("/echo")) {
                    return CompletableFuture.completedStage(OrbitResponse.streaming(200,
                            List.of(Header.text("content-type", "application/octet-stream")), writer ->
                            CompletableFuture.runAsync(() -> {
                                for (;;) {
                                    NativeBuffer chunk = request.readBody(8192).completion().toCompletableFuture().join();
                                    if (chunk == null) break;
                                    try (chunk) {
                                        writer.write(chunk.view().toArray(ValueLayout.JAVA_BYTE)).toCompletableFuture().join();
                                    }
                                }
                            }, handlers)));
                }
                String body;
                switch (request.uri().getRawPath()) {
                    case "/fail" -> throw new IllegalStateException("Example handler failure");
                    case "/publish" -> {
                        application.publish("example.local", "{\"instance\":\"" + instance + "\"}");
                        body = "published";
                    }
                    case "/a%2Fb" -> {
                        Header raw = request.headers().stream().filter(header -> header.name().equalsIgnoreCase("x-raw"))
                                .findFirst().orElseThrow();
                        body = request.method() + " " + request.uri() + " " + Integer.toHexString(Byte.toUnsignedInt(raw.value().get()));
                    }
                    default -> body = "Hello, Java!";
                }
                return CompletableFuture.completedStage(OrbitResponse.text(200, List.of(
                        Header.text("content-type", "text/plain"), Header.text("x-java-instance", instance),
                        Header.text("set-cookie", "a=1"), Header.text("set-cookie", "b=2"),
                        Header.copyOf("x-raw", new byte[]{(byte) 255})), body));
            }, handlers);
            System.out.println("READY " + instance);
            System.out.flush();
            BufferedReader control = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            for (String line; (line = control.readLine()) != null && !line.equals("stop"); ) {
                switch (line) {
                    case "unsubscribe-edge" -> {
                        application.unsubscribe("example.edge");
                        System.out.println("UNSUBSCRIBED " + instance);
                    }
                    case "publish-local" -> {
                        application.publish("example.local", "{\"instance\":\"" + instance + "\"}");
                        System.out.println("PUBLISHED " + instance);
                    }
                    default -> throw new IllegalArgumentException("Unknown example control");
                }
                System.out.flush();
            }
        }
        System.out.println("DRAINED " + instance);
    }
}
