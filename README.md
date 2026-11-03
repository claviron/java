# orbit-java

Framework-independent Java application bindings for Claviron's same-host Orbit
Link. The Java application runs in a separate JVM. The native library consumes
Claviron through its public facade; Claviron does not embed Java or depend on
this binding. This source tree is not published yet.

The implementation joins a named application as a `java` lane, serves HTTP with
incremental uploads and streaming responses, and shares ApplicationEvents. It
supports application generations and bidirectional invokes on a separate,
application-owned Link. Close drains owned work. The binding uses JDK FFM,
without JNI, preview APIs or a Java framework.

## Requirements and validation

- JDK 25 LTS.
- Stable Rust 1.99 or later and a C11 compiler.
- Python 3 for the dependency-free build/check scripts.
- Linux x86-64 is the currently validated native/FFM platform.

Set `JAVA_HOME` to a JDK 25 installation, then run:

```sh
python3 scripts/test.py
python3 scripts/application-test.py --skip-build
python3 scripts/streaming-test.py --skip-build
python3 scripts/lifecycle-test.py --skip-build
python3 scripts/invoke-test.py --skip-build
```

The first script checks Rust formatting, lints and tests the workspace, builds
the shared libraries, compiles Java with warnings treated as errors, packages
the production JAR, and checks FFM behavior and C ABI layout. It starts no server,
fleet or load test. `CARGO_TARGET_DIR`, `JAVA_HOME` and `CC` are respected; PATH
is not rewritten. `--skip-rust-checks` reuses already completed Rust checks.

The application script requires the `orbit` CLI on PATH for cleanup. It starts
one Rust acceptance peer and two JVM applications in fresh, isolated SHM fleets.
It checks HTTP on both instances, raw URI/query and ordered byte headers, handler
failure, idle peer cancellation, sibling event sharing, Edge event forwarding,
unsubscribe and graceful drain. It opens no public listener and applies no load.
Only its own fleets are cleared, after all its child processes have exited.
Omit `--skip-build` to run foundation checks and the build first.

The streaming script uses another fresh fleet and a separate JVM. A response
larger than Link's configured ring must remain pending while its reader is held
back. It checks byte ownership, rejection of concurrent reads/writes, capacity
rejection without corrupting stream state, cancellation of blocked operations,
producer failure after the head, empty-body EOF, large binary full-duplex echo,
and application close while accepted upload/response work is still pending.
Readiness and explicit control messages coordinate these cases; no sleeps,
invented deadlines or load tests are used.

Artifacts:

- `target/debug/liborbit_java.so`: production native binding.
- `target/debug/liborbit_java_fixture.so`: test-only producer.
- `java/build/orbit-java.jar`: production Java classes, without fixture/test code.

The runtime needs an explicit native-access grant. On the class path:

```sh
"$JAVA_HOME/bin/java" --enable-native-access=ALL-UNNAMED \
  --illegal-native-access=deny -cp orbit-java.jar:application.jar example.Main
```

Pass the library path to `NativeLibrary.load(Path)`. There is no automatic native
library download, runtime discovery or platform fallback.

## Application API

The application supplies every operational budget: completion operations,
retained body bytes, completion workers, retained HTTP exchanges, pending
event delivery and pending control notifications. Joining requires a fleet name,
application name and nonzero incarnation. A fleet name is limited to eight bytes
by the underlying protocol.
No request timeout, drain deadline or process termination policy is invented.

```java
try (NativeLibrary library = NativeLibrary.load(libraryPath);
     NativeRuntime runtime = library.openRuntime(operationBudget, byteBudget, completionWorkers);
     var executor = Executors.newVirtualThreadPerTaskExecutor();
     OrbitApplication application = runtime.join(new ApplicationOptions(
             fleet, name, incarnation, exchangeBudget, eventBudget, controlBudget))) {
    application.subscribe("example.changed");
    application.events(event -> consume(event.topic(), event.json()), executor);
    application.serve(request -> CompletableFuture.completedStage(
            OrbitResponse.text("Hello, Java!")), executor);
    waitForApplicationShutdown(); // The embedding application's own signal.
}
```

The example is schematic: supply the declared variables, imports and application
callbacks. A complete executable plain Java handler is in
[examples/plain/PlainApplication.java](examples/plain/PlainApplication.java).
`scripts/application-test.py` compiles and exercises it with explicit fixture
budgets. No framework executor or common pool is chosen by the binding.

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

`scripts/lifecycle-test.py` runs two sibling JVMs and one unrelated application in
a fresh fleet. It checks unchanged process/lane/incarnation identity, exact target
filtering, stopped-request 503 without handler calls, old signal cancellation,
idempotent start, restart of blocked reads/writes, ignored late responses, local
restart fanout, status generations and closure with cancelled handler futures
still pending. A blocked user-event callback with a full delivery queue remains
blocked through stop/start, proving independent control progress. All owned
processes exit before cleanup of that test fleet. No load test is involved.

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
Claviron and Orbitive need no source changes for this mode. The local orbit-js
implementation supplies lifecycle behavior as a reference, but has no invoke API
to copy.

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

`scripts/invoke-test.py` starts a plain JVM and Rust peer with fresh application
and invoke fleets. It verifies both call directions, binary/UTF-8 operation data,
error distinctions, capacity rejection, caller cancellation without remote
rollback, generation cancellation in both directions, ignored late replies,
HTTP progress while invoke is saturated, unchanged endpoint incarnation and a
graceful nested invoke drain. Only its own exited children/fleets are cleaned up;
no load test is run.

## FFM ownership and completion

The C ABI is version 3. Application options now require `controlCapacity`, request
metadata includes its generation, and control/state records have checked layouts.
Rebuild the native library and JAR together; older ABI versions are rejected.
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

See [the C header](native/include/orbit_java.h),
[architecture](ARCHITECTURE.md) and [implementation plan](PLAN.md) for contracts
and remaining work.
