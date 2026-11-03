# orbit-java architecture

The JVM is a separate application process. The native library consumes Orbit
through the public Claviron facade. Application transport (HTTP and application
events) is the first connection mode; the invoke command/result protocol uses a separate application-owned service
Link. It never adds a second reader to an application lane. orbit-js is a behavioral reference, never a dependency.

## Application transport implementation

A process joins the named application's fleet with its own `java` lane. One
native current-thread Tokio host drives a Continuo Runnable (through Claviron's
runtime facade). Its RunContext cancels admission and owns graceful drain; it
neither boots the Claviron system nor owns the JVM. Explicit application limits
bound retained exchanges and pending event delivery. FFM operation and byte
budgets remain distinct bridge resources.

The driver exclusively reads the application inbox, using Claviron's existing
TransportSegment, TransportBodies, frame and application-event codecs. It never
implements SHM geometry, resets a fleet or unlinks shared resources. Each accepted
exchange retains its request metadata, duplex endpoints and application owner.
A response has one final head, then either a complete owned body or incremental
writes followed by explicit finish. An output actor owns WriteHalf and a gate
admits only one pending output operation; its single command slot is not a chunk
queue. Head, write and finish transitions are checked before admission; rejected
operation/byte reservations restore the prior phase. A pending write owns copied
input until all bytes enter Link or cancellation resets the exchange.

Request reads independently take exclusive ownership of ReadHalf for one async
operation. A second read fails Busy rather than registering another waiter. The
requested capacity is reserved before allocation/read; a successful short read
transfers its storage into a buffer lease exposing only the actual length. That
lease retains its full capacity reservation until release. EOF transfers no lease.
Cancellation settles once and wakes the read; native storage remains owned by the
read task even if Java has already retired its cancelled operation. A late result
cannot create an unobserved buffer. Cancelling a read abandons the exchange,
because bytes may already have been consumed when cancellation won.

The output actor watches client departure even while the producer is idle. It
never competes with a pending stream write for the same readiness registration.
A producer/write failure resets the stream; normal finish sends FIN and releases
unread input separately. A non-cancelled producer retains its Java request until its
stage and native terminal cleanup finish. Generation/peer cancellation instead
retires bridge ownership independently of a Java future that ignores its signal. Producer code runs outside request
monitors. No forced Java cancellation, timer, polling interval or automatic
whole-body buffering is introduced.

Application event sharing has two separate paths: the application's fleet ring
between sibling lanes, and LinkEvents forwarding between the application and
Edge fleets. Each process owns its subscription cursor and interest. The native
LinkEvents bridge retains origin-only forwarding and anti-echo rules; Java never
forwards a received event again. The receiving lane republishes once into the
application ring; every subscriber (including that lane) consumes via its own
cursor. Explicit local publication also enters that ring once. This avoids both
missing delivery to the receiving process and duplicate direct/ring delivery.

Withdrawal stops new admission, closes pending intake, and refuses queued
undelivered requests. Java waits for accepted handler/response cleanup and delivered event scopes.
Accepted event callbacks may finish publishing during drain; the native driver
flushes their local publications before stopping event forwarding. The
native driver drains owned I/O, retains its lane hold through drain, and signals
completion. Release then joins the native host. Runtime/library release is Busy
while application owners remain. Peer departure wakes response ownership and
is distinct from normal FIN. Edge shutdown is a notification, not JVM termination.


## Native ownership

`native` owns the versioned C ABI, typed resource registry, byte reservations and
completion queue. `native/addon` exports that ABI as the production shared library.
`harness/native` exports the same ABI plus controlled test producers. Test symbols
are absent from the production library. The fixture never boots a server or fleet.

Handles are process-unique, monotonically allocated integer identities. Registry
entries carry their resource kind. They are never pointers or recycled slots.
Operations retain their engine; payloads reserve its byte budget; returned buffer
leases retain the engine independently of operation handles. Input is copied
before submission returns. Completion transfers a payload without another copy.

Every admitted operation reserves one completion credit, held until its delivered
operation handle is released. Thus the queue and Java delivery queue cannot exceed
the configured operation budget. Cancellation uses the same reserved terminal
slot, not an additional control queue. Shutdown is a state flag and wake, requiring
no queue space. Operation and byte limits are required inputs, with no defaults.

One operation settles once: pending -> queued terminal -> delivered -> released.
Cancellation and producer completion race under its state lock. Cancellation loses
after a terminal result is queued. Release before delivery fails with Busy. A
buffer becomes separately owned at delivery and is released explicitly.

The engine completion waiter parks on a condition variable. Producers and shutdown
wake it; there are no timeouts, sleeps or polling. There is one consumer per engine.
Shutdown rejects submissions and cancels pending operations; queued successful
results retain their result ownership. Wait returns End only after all pending
operations settle and queued results are drained. This engine shutdown is not the
application's graceful admission-close API.

Engine release requires shutdown, no waiter, no operations, no buffer leases and
no attached applications. Exchange and delivered-event owners retain their
application; a delivered event releases its native scope explicitly after its
callback finishes.
Failed release keeps the handle valid. Lookup clones an Arc before releasing the
registry lock. Removal drops owners outside that lock. No Rust unwind crosses C;
panic is an explicit status, not a promise to recover a poisoned runtime or abort.
C callers must obey the header's valid-pointer, alignment and aliasing contract.

## Java ownership and delivery

Java's private FFM layouts are checked against native sizes/alignment before an
engine opens. JDK 25 is required without preview or JNI. `NativeLibrary` owns its
library-lookup arena and refuses close while an engine remains attached.

`NativeRuntime` owns one platform waiter and an explicitly sized completion-worker
group. The waiter only drains results and enqueues dispatch; user-dependent future
callbacks triggered by delivery execute on completion workers. Late registrations
follow normal CompletionStage caller-thread semantics. A worker releases its
operation credit before completing the future, so chained submissions can progress. A blocked callback occupies only that completion
worker; queued and pending operations remain credit-bounded. Submission and
result registration are serialized against early delivery and close. Completion
workers may not synchronously close their own runtime.

`NativeBuffer` owns a native buffer handle and a shared scoped, read-only FFM view.
Close invalidates Java aliases through the arena before releasing native storage.
It is idempotent. Runtime close joins waiter and dispatch workers, then attempts
engine release. Unclosed leases cause Busy rather than invalidating their memory;
close can be retried after releasing them. Library close never unloads active code.
No cleanup correctness depends on finalizers, Cleaner, process exit or GC.

Production application operations include request/event intake, response
completion, terminal observation, generation retirement, control delivery and
application drain. ABI version 3 keeps buffer/resource completions and adds an
explicit control budget, generation request metadata and checked control/state
records. The Java/native pair rejects mismatched ABI versions before joining.
The separate test fixture still admits controlled byte payloads and cannot be
loaded as a production producer by accident.

## Lock order and tests

Registry lookup never holds its lock while acquiring an engine/operation lock.
Submission holds the engine lock while publishing a new registry entry. Settlement
holds an operation lock, publishes its buffer, then acquires the engine lock.
Shutdown snapshots pending owners under the engine lock and cancels after unlocking.
Wait removes a queue record before locking its operation. No path locks an existing
operation while holding the engine lock.

Rust tests live under `native/tests` and mirror modules, included from their owning
module. Java checks live under `java/src/test/java` and use deterministic gates.
The native fixture and C layout probe live under `harness`. No network/load tests
or cross-platform support are inferred from the Linux x86-64 foundation checks.
The explicit application harness separately exercises real SHM request/response
and both event paths across two JVM processes, without booting the whole system.
Its HTTP-only Edge lane leaves before the event phase: every remaining lane
advertising the far-side role must be able to accept and bridge event frames.

## Streaming acceptance

The explicit streaming harness uses a separate JVM and a real Link caller, each
with its own lane. The caller supplies the configured ring geometry to the Java
fixture, which writes more than the ring can retain without a reader. A control
handshake holds that reader until the Java side verifies a pending write and
Busy rejection. Other cases check capacity rejection/rollback, blocked read and
write cancellation, producer failure after head, empty EOF, full-duplex binary
echo larger than either direction's ring, and close during an accepted upload.
The fixture and its assertions stay outside the production JAR. Unique fleets
are removed only after both owned processes exit; no production service or
listener is used. These are correctness checks, not throughput measurements.

## Application lifecycle ownership

Membership incarnation belongs to Link, whereas application generation belongs
to the managed handler scope. Lifecycle state starts running at generation 1.
A single mutex serializes state transitions and notification ordering. Stop
cancels the current shared Generation token without leaving the lane. Start after
stop and every restart allocate a new monotonically checked identity/token; start
while running preserves both. Exhaustion fails without wrapping or partially
changing the existing generation.

Each accepted exchange retains its Generation Arc. Its idle output wait, pending
read and pending write select on that token. Admission while stopped refuses 503;
queued retired exchanges are discarded before handoff. Java also checks current
ownership before invoking the handler. One retire gate per Java handler releases
the request/native terminal ownership exactly once on completion or cancellation.
A response returned after retirement is never started. Native output operations
check the retained token, so old work cannot send through a new generation.
Arbitrary Java computation is cooperative and remains the caller's responsibility.

Edge actions require the exact java/application target. The receiver publishes a
targeted internal lifecycle ApplicationEvent, applies locally once, and skips its
own copy in the internal ring cursor. Siblings apply it independently of user
subscriptions. Accepted Link lifecycle events apply at the receiving node and
share through the ring. The internal topic is separate from public event delivery;
its independent cursor/readiness cannot stall behind a full application-event
queue. Origin/anti-echo forwarding stays in the existing LinkEvents implementation.
There is no durable cohort state or global concurrent-publisher order introduced.

A bounded notification queue carries immutable lifecycle/command/shutdown values.
Overflow is an observable application failure that cancels its generation and
withdraws admission; it is never ignored or replaced with an invented retry timer.
The automatic Java control reader copies a native ControlRecord, invokes an
optional callback on the supplied executor, then releases its scope. Event and
control scopes share callback-drain accounting but remain typed registry handles.
A callback sees its transition snapshot; a live application getter may be newer.

Generation observers are tracked separately from native I/O drain. They complete
on retirement or final close, so merely observing a current token cannot keep
close waiting for itself. Graceful withdrawal does not retire healthy accepted
HTTP work; stop/restart does. No process signal, JVM exit, classloader restart or
framework lifecycle is implied by application management. Edge shutdown is a
separate notification, not a stop command.

The explicit lifecycle harness exercises two sibling JVMs and a third unrelated
application with unchanged PIDs, lanes and incarnation. It verifies fanout once,
current status generations, exact targets, stopped admission, cancellation of
blocked duplex operations, late response suppression, control progress through
full event queues and close with ignored cancelled futures. No system boot or
live production service is used.

## Application-owned invoke connection

Invoke is opt-in and uses the core `invoke::Client` and `link::invoke::Driver`.
The application supplies a separate fleet, service identity, route map and
pending/accepted/byte bounds. The existing Claviron LinkSpec provides compatible
geometry in that isolated fleet; no SHM kind or private codec is invented. Each
service connection has one reader. No HTTP Exchange or ApplicationEvent is used
to package a call. Isolation also keeps non-HTTP lanes out of the HTTP event
bridge's far-side lane selection.

The binding starts its driver on the parent's native host and publishes startup
only after polling the core driver into enabled admission. It never assumes a
yield or invented delay establishes readiness. Driver work stays tracked by the
application's native I/O drain. Parent withdrawal marks invoke admission closing,
refuses queued undelivered requests, and lets accepted incoming/outgoing work
drain through the core protocol before connection release. Close observers use
the parent's separate control tracker, avoiding a self-drain dependency.

Incoming records own copied/leased metadata, a captured Generation Arc, reply
oneshot and terminal state. The core validates destination incarnation and claims
the caller's reserved stream before domain dispatch. Generation retirement races
the reply wait; Java uses an exact-once retire guard and ignores late replies.
Successful reply handoff is recorded before Java request release, so normal
release cannot falsely cancel a committed reply. The core driver retains result
write/backpressure ownership until its job drains. Accepted-call scope and wire
result ownership remain distinct.

Caller cancellation drops only the local core call future and result reader;
there is no remote cancel/rollback frame. Its operation token and parent generation
wake the adapter without polling. The remote service may complete accepted work.
Unknown outcomes and typed remote failures remain distinguishable. Routes are
immutable logical bindings, with exact-one endpoint selection and no implicit
load balancing, fallback or retry. The HTTP connection remains independent when
a bounded invoke endpoint is full or waiting for a handler.

Outgoing result capacity is reserved before submission without allocating a
second result array. The core-owned result Vec transfers into an engine-pinned,
typed ResultRecord. Java copies the result once and releases that resource during
completion transformation. Cancellation that wins against a late completion
releases the result resource instead of stranding it. Payload accounting tracks
reservation ownership even after a byte buffer moves into the protocol. Core
wire/accepted-job storage has its own configured bounds; it is not counted as
FFM-retained buffer memory. Empty results also pin their engine until release.

ABI 3 gains an additive invoke capability and separately queried fixed C layouts.
Invoke binding, incoming invocation and completed result are distinct typed handle
kinds. The Java API separates InvokeCall/InvokeResult, OrbitInvocation/InvokeReply
and HTTP types. No JNI, framework adapter, implicit timeout or JVM lifecycle hook
is added. Application close joins child bindings before releasing the native host.

The real invoke harness runs a JVM and Rust service on separate fresh fleets.
It tests both directions, typed failures/bounds, caller abandonment without
rollback, local generation cancellation, stale replies, independent HTTP progress
and a nested outgoing/incoming call that completes during application close.


## Maven distribution and packaged native loading

The Java API is a platform-neutral Java 25 JAR. Five native classifier JARs
carry only the corresponding library and immutable release metadata. Java
package names and Maven group coordinates are independent. The main artifact
contains API version/ABI metadata; a selected native resource contains matching
version/ABI/classifier metadata and the library SHA-256.

`NativeLibrary.load()` selects the current supported OS/architecture, requires
exactly one matching classpath resource, validates metadata and copies the
library into a private temporary directory while hashing it. FFM then checks the
actual C ABI as for explicit `load(Path)`. There is no runtime download, system
search or platform fallback. `load(Path)` retains operator-controlled loading.
The library owner keeps extracted files until its FFM arena closes; active
runtimes refuse close. Failed loading removes its extraction. Cleanup failures
remain observable and can be retried by closing the library again.

The release workflow uses published Cargo dependencies in an isolated source
snapshot and a shared ignored release lockfile. Native receipts bind sources,
dependencies, target, portable CPU profile and binary checksum. Remote workers
build isolated committed clones and return only verified prebuilds. Package
assembly produces the common JAR, source/Javadoc JARs, POM and classified native
JARs without bundling fixture code. Partial previews cannot be uploaded.
Signing and a USER_MANAGED Portal upload are separate explicit commands; upload
does not request automatic publication. Packaging tests use an ephemeral key
and mock the upload endpoint, without a real deployment.
