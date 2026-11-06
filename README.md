# claviron/java

A Java library for handling Claviron application requests over same-host Orbit
Link. Your normal Java application imports the library and supplies a request
handler. Claviron owns public connections and host routing; your application
owns its handlers, threads and JVM lifetime.

## Current stage

HTTP with incremental uploads and streaming responses, shared application
events, start/stop/restart with generation cancellation, and bidirectional
invokes on a separate Link are supported. The binding uses JDK FFM, without JNI,
preview APIs or a Java framework. Shared cells and sessions remain a later stage.

End-to-end validation covers Linux x86-64. macOS arm64 is checked to load, join
and close; the other platforms are built and checked but have not been run.

## Install

Requires JDK 25 LTS. Add the API and the native library for your platform, one of
`linux-x86_64`, `linux-aarch64`, `macos-aarch64`, `freebsd-x86_64` or
`freebsd-aarch64`.

Gradle:

```kotlin
dependencies {
    implementation("dev.claviron:orbit-java:0.1.0")
    runtimeOnly("dev.claviron:orbit-java:0.1.0:linux-x86_64")
}
```

Maven:

```xml
<dependency>
  <groupId>dev.claviron</groupId><artifactId>orbit-java</artifactId><version>0.1.0</version>
</dependency>
<dependency>
  <groupId>dev.claviron</groupId><artifactId>orbit-java</artifactId><version>0.1.0</version>
  <classifier>linux-x86_64</classifier><scope>runtime</scope>
</dependency>
```

The next release moves to the coordinates `dev.claviron:orbit`.

## Use from a Java application

```java
import dev.claviron.orbit.ApplicationOptions;
import dev.claviron.orbit.NativeLibrary;
import dev.claviron.orbit.OrbitResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public final class Main {
    public static void main(String[] args) throws Exception {
        var options = new ApplicationOptions("example", "hello", 1, 64, 64, 64);
        try (var library = NativeLibrary.load();
             var runtime = library.openRuntime(1024, 16 * 1024 * 1024, 2);
             var handlers = Executors.newVirtualThreadPerTaskExecutor();
             var app = runtime.join(options)) {
            app.serve(request -> CompletableFuture.completedStage(
                    OrbitResponse.text("Hello from Java")), handlers);

            // Your application's lifecycle decides when to withdraw and drain.
            Thread.currentThread().join();
        }
    }
}
```

Run it with native access granted:

```sh
java --enable-native-access=ALL-UNNAMED -cp "libs/*:." Main
```

See the complete [Java example](examples/plain/PlainApplication.java). The
library installs no signals and does not own `main`. `"example"` is the fleet, 1–8
bytes, and `"hello"` the application name; a Claviron edge on the same host routes
to them. `1` is the incarnation. The caller supplies every budget:

- `ApplicationOptions`: retained HTTP exchanges, pending events, pending controls.
- `openRuntime`: native operations, retained body bytes, completion workers.

`NativeLibrary.load()` finds the matching native JAR on the class path and checks
it. Nothing is downloaded at run time. No request timeout, drain deadline or
process termination policy is invented.

Requests expose the accepted method, absolute URI, HTTP version, repeated byte
headers and peer-cancellation stage. Handler exceptions before a response is
selected produce HTTP 500. Unsupported upgrades are refused.

## Request bodies and streaming responses

`request.readBody(maxBytes)` returns an explicitly cancellable `NativeOperation`.
Its completion returns a `NativeBuffer` containing up to that many bytes, or
`null` at normal EOF. It does not wait to fill the requested capacity. Close every
returned lease. The whole requested capacity is reserved against the runtime's
byte budget until that lease is released; only the actual read bytes are exposed.
A buffer may outlive its request, but keeps its own runtime byte reservation.

There is at most one pending read per exchange. A second read is rejected with
Busy. A normal upload FIN is EOF, not request cancellation. Cancelling the native
read operation abandons the exchange: a read racing cancellation may already have
consumed bytes, so retrying must not silently lose part of the input. Cancelling
a derived CompletableFuture does not cancel native work.

A response may contain immutable bytes or a streaming producer:

```java
OrbitResponse.streaming(200, List.of(Header.text("content-type", "text/plain")),
        output -> output.write(firstChunk)
                .thenCompose(ignored -> output.write(secondChunk)))
```

The binding commits the head once, invokes the producer, and sends FIN only after
the producer's stage succeeds. The producer must await each write before issuing
the next and complete only after its last write. There is no unbounded chunk
queue: concurrent output operations return Busy. Input reads and output writes
can progress independently on the same exchange.

`output.write(byte[])` copies input before returning and completes only when all
bytes have entered the bounded Link stream. It may stay pending while the client
does not read. Completion does not mean the remote client consumed those bytes.
`output.finish()` is automatic for producer responses; manual callers use it
explicitly. Writes after finish are rejected. `output.abort()` abandons only this
exchange and wakes its blocked I/O. Peer departure also wakes blocked operations
and the request cancellation stage.

A producer exception or failed/cancelled write resets output. Failure after the
head is never replaced by a second HTTP 500 or a successful EOF. A successful
response releases unread upload ownership; that cleanup does not turn a normal
completed response into request cancellation. Retirement releases the bridge even
if the handler or producer ignores its cancellation signal. That Java computation
still belongs to its executor. A healthy producer that never completes can delay
graceful close indefinitely. Supply an asynchronous producer; blocking work
belongs on the application's own executor.

`serve` owns request release and waits for producer/response/terminal cleanup.
Manual `nextRequest` callers own the request and must close it. They can use
`beginResponse(status, headers)` to obtain a writer, then write, finish and close
the request. Calling `respond(response)` drives a static or producer response.
Every path preserves typed native ownership and the explicit runtime budgets.

The executable plain Java example includes an `/echo` handler that incrementally
reads an upload and streams it back on a supplied virtual-thread executor. It
retains one read lease and one pending write, without buffering the whole body.

## Events and lifecycle

Events are part of the application binding. Subscribe explicitly by domain topic;
`publish(topic, json)` creates an ApplicationEvent in the application's ring.
Each JVM has an independent subscription cursor. Sibling instances share that
ring. A separate LinkEvents bridge carries publications between application and
Edge fleets using the existing routing, origin and anti-echo rules.

An incoming Edge event is republished once into the application ring. The
receiving JVM consumes through its ring cursor, like its siblings, rather than
calling the subscriber both directly and through the ring. Forwarding is bounded
and has no durable replay or remote acknowledgment guarantee. Ring lag is an
observable delivery failure. Interest filters routing; it is not authorization.
An Edge lane participating in event routing must have its event bridge attached.

`events(listener, executor)` delivers sequential callbacks and closes each native
event scope after the callback returns. Manual `nextEvent` callers must close the
returned `OrbitEvent`; its copied metadata remains readable afterwards. An
accepted callback retains its application through drain. A blocked callback can
therefore delay close indefinitely.

`withdraw()` stops admission and wakes pending intake. `close()` joins accepted
HTTP response ownership and event/control callbacks, drains native I/O, releases
membership and joins the native host. Accepted callbacks may finish publishing while draining.
Call close outside those callbacks; withdrawal is safe inside them. Application
close does not terminate the JVM or clear shared memory. Runtime/library close
is refused while child application owners remain.

The native driver is a Continuo Runnable through Claviron's runtime facade; it
runs on one owned current-thread Tokio host, without booting an entire Claviron
system. Status uses the existing Java application description/schema. Shared
cells, sessions and generic service mode remain subsequent work. No framework
adapter is included.

## Application lifecycle and generations

`start()`, `stop()` and `restart()` manage the application while keeping the JVM,
Link lane and process incarnation unchanged. Edge commands use the existing
ControlCommand frame and must target runtime `java` and this application's name.
One receiving instance shares the decision with its siblings through a targeted
internal ApplicationEvent. Other application names/runtimes ignore it. These are
live-cohort notifications, not durable state for a later-joining JVM.

- Joining starts running at generation 1, independently of the process incarnation.
- Stop cancels the current generation. New requests receive 503 before the handler;
  the lane stays present for subsequent commands and events.
- Start after stop creates the next live generation. Start while running preserves
  the current generation and its cancellation signal.
- Restart cancels the old generation and creates a new running generation, whether
  the application was previously running or stopped.

`application.generation()` captures an `ApplicationGeneration`: its `id()` remains
stable and its `cancelled()` stage completes when retired or after application
close. Captured old signals never become live again. Requests capture native
generation ownership at admission and expose their own `generation()` number.
Their cancellation stage includes generation retirement as well as peer departure.
A late handler cannot publish into a new generation. Cancelled bridge work retires
without waiting for an ignored Java future; arbitrary Java tasks are not killed.

`controls(listener, executor)` installs one consumer for lifecycle, opaque command
and named Edge-shutdown snapshots. Transitions and new native generation ownership
are applied before notification. Each `OrbitControl` includes its transition's
number/running state; `application.generation()` and `running()` query the latest
authoritative state, which can be ahead if several commands arrive before a
callback runs. The automatic control reader also runs without a user listener.
Control callbacks execute sequentially on the supplied executor and retain their
native scope until return. An Edge shutdown notice alone stops neither the whole
application nor the JVM.

Lifecycle ring consumption has independent readiness/cursor ownership, so a full
user-event queue cannot delay generation retirement. Control notification capacity
is explicit. Exhaustion or a lost lifecycle cursor is an observable application
failure and withdraws admission; decisions are never silently dropped. The shared
ring does not establish a global order for concurrent commands from independent
publishers. Send dependent management decisions sequentially.

Graceful `withdraw`/`close` preserves accepted, non-cancelled HTTP work for drain;
application stop/restart explicitly retires its generation. Close joins the native
host, not the JVM or an executor owned by the application. Call close outside
handler/event/control callbacks; those callbacks can safely request withdrawal.

## Invokes on an application-owned service Link

Invoke is a separate command/result protocol, not an HTTP request. Open it once
while preparing the application:

```java
OrbitInvocations invokes = application.invocations(new InvokeOptions(
        invokeFleet, serviceName, pendingCalls, acceptedCalls,
        operationBytes, requestBytes, responseBytes,
        Map.of("calculator", "math")));

invokes.serve(request -> CompletableFuture.completedStage(
        InvokeReply.success(handleOperation(request.operation(), request.payload()))), executor);

InvokeCall call = invokes.call("calculator", "sum", encodedInput);
CompletionStage<InvokeResult> result = call.completion();
```

The snippet is schematic: supply the declared bounds, names, imports and domain
codec/handler. `InvokeReply` supports success, busy, closing, unsupported,
response-too-large and opaque rejection bytes. A thrown handler or failed/null
handler stage becomes an empty rejection, with no HTTP status or exception text
on the wire. `InvokeResult` distinguishes these remote replies from unknown
logical target, ambiguous endpoint, unavailable endpoint, local capacity,
protocol/transport failure and outcome unknown. Its read-only payload is owned by
Java and needs no native lease close. No serialization format or framework is
selected; payloads are opaque bytes.

`invokeFleet` is an operator-supplied fleet distinct from the application's
HTTP/event fleet. The separate connection has its own exclusive reader, admission
and stream slots. It uses the existing Claviron Link geometry/protocol; peers must
agree on that compiled geometry. Adding invoke never attaches a second reader
to an HTTP lane and cannot place an unbridged invoke lane in the HTTP event
bridge's routing set. It opens no invocation broadcast ring or public listener.
The binding uses `claviron::orbit::invoke::Client` and the core Link route/driver;
Claviron and Orbitive need no source changes for this mode. Orbit Java and
Orbit JS use the same separate-Link lifecycle and local-cancellation contracts.

Routes map logical targets to Link services once at creation. A service must have
exactly one live endpoint; duplicates return `AMBIGUOUS_TARGET`. There is no
implicit round-robin, failover, retry or HTTP fallback. Multiple instances need
explicit distinct service identities/routes or a separately designed placement
policy. Invoke pending/accepted counts and operation/request/result sizes are
explicit and independent of HTTP exchange limits. Results use the core protocol's
reserved, correlated stream channel, so inbox contention cannot consume response
capacity. Native Link backpressure still applies.

`call.cancel()` withdraws local interest and completes its Java stage with
CancellationException. Cancelling a derived CompletableFuture does not cancel
the native call. Cancellation is not remote cancellation or rollback: accepted
remote work may finish after its caller stops waiting. `OUTCOME_UNKNOWN` must not
be treated as a safe retry. Domain authorization, commit IDs and outcome queries
belong to the handler; a Link peer stamp is not an authenticated user identity.

Every incoming/outgoing invoke captures its parent application's native generation.
Stop/restart cancels that application's waits and accepted invoke bridges. Stopped
incoming work receives Closing, while the connection/PID/incarnation stays up for
start. Late handler replies cannot enter a new generation. A remote service's
accepted work remains its own responsibility; generation cancellation does not
terminate that other service. `OrbitInvocation.cancelled()` exposes local scope
retirement, not whether the remote caller still waits.

`OrbitInvocations.serve` owns incoming request release and drains healthy accepted
work. Manual `next()` consumers must reply/close `OrbitInvocation`; its metadata
is copied into Java. The parent application closes and joins its invoke children;
closing a child alone stops only that invoke connection. Graceful application
withdrawal drains existing calls in both directions, including nested invoke
chains, without inventing a deadline. Close outside handler callbacks. A handler
that ignores generation cancellation may continue computing in its own executor,
but cannot retain a retired native reply scope.

Outgoing calls reserve their configured result capacity against the FFM byte
budget before publication, without allocating a duplicate result buffer. The
reservation remains until Java copies/releases the typed result resource. Input
bridge ownership and incoming metadata also use the FFM byte budget. Core
request/result encoding and write storage remain separately bounded by the
configured invoke limits and accepted-call count; the FFM byte budget is not a
process RSS bound. No end-to-end zero-copy claim is made.

## FFM ownership and completion

The C ABI is version 3. Application options now require `controlCapacity`, request
metadata includes its generation, and control/state records have checked layouts.
Use the native library and JAR of one version together; older ABI versions are rejected.
`OJ_CAP_STREAMING`, `OJ_CAP_LIFECYCLE` and the additive `OJ_CAP_INVOKE`
describe supported operations. Invoke layouts have their own checked query;
existing ABI 3 records are unchanged by invoke support.
Layout/capability validation happens before resource creation. Handles
are typed, non-recycled integer identities. Input bytes become native-owned
before submission returns. A stale or wrong-kind handle is an error.

One Java platform thread blocks in the native completion wait. Native producers,
cancellation and shutdown wake it without polling or timers. Results dispatch
to a bounded group of completion workers. Operation credit retires before user
CompletionStage callbacks, allowing chained submission. A blocked callback does
not block the native waiter; blocking every completion worker stalls delivery.

`NativeOperation.cancel()` cancels pending native work. Cancelling a derived
CompletableFuture does not cancel that operation. Cancellation loses after a
terminal result is queued. No Java computation is forcibly stopped.

`NativeBuffer` is AutoCloseable. Closing invalidates its scoped read-only Java
views before native storage is freed. Buffer byte reservations and operation
credits have separate owners. Cleanup does not depend on GC or Cleaner.

`NativeRuntime.shutdown()` cancels pending operations and preserves queued
results. Close joins completion threads before engine release; a retained buffer
makes release Busy and close can be retried after releasing it. A runtime cannot
close synchronously from its own completion callback. Library unload is refused
while any runtime is attached. Unrecoverable ABI/driver failures retain live
resources rather than unloading code still in use.

See [the C header](native/include/claviron_java.h) and
[architecture](ARCHITECTURE.md) for contracts.
