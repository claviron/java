package io.claviron.orbit;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/** Audited application descriptors and borrowed metadata copies, private to the binding. */
@SuppressWarnings("restricted")
final class ApplicationFfm {
    private static final MemoryLayout OPTIONS = MemoryLayout.structLayout(
            JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, Ffm.VIEW, Ffm.VIEW);
    private static final MemoryLayout INFO = MemoryLayout.structLayout(JAVA_INT, JAVA_INT, Ffm.VIEW, Ffm.VIEW, JAVA_LONG, JAVA_LONG);
    private static final MemoryLayout HEADER = MemoryLayout.structLayout(Ffm.VIEW, Ffm.VIEW);
    private static final MemoryLayout EVENT = MemoryLayout.structLayout(Ffm.VIEW, Ffm.VIEW, Ffm.VIEW);
    private static final MemoryLayout STATE = MemoryLayout.structLayout(JAVA_LONG, JAVA_LONG);
    private static final MemoryLayout CONTROL = MemoryLayout.structLayout(JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG, Ffm.VIEW, Ffm.VIEW, Ffm.VIEW);
    private final MethodHandle state;
    private final MethodHandle current;
    private final MethodHandle action;
    private final MethodHandle generationWatch;
    final MethodHandle controlNext;
    private final MethodHandle controlInfo;
    private final MethodHandle controlRelease;
    private final Ffm ffi;
    private final MethodHandle join;
    final MethodHandle next;
    final MethodHandle closed;
    final MethodHandle withdraw;
    final MethodHandle release;
    final MethodHandle requestRelease;
    final MethodHandle watch;
    private final MethodHandle requestInfo;
    private final MethodHandle requestHeaders;
    private final MethodHandle respond;
    private final MethodHandle head;
    private final MethodHandle write;
    final MethodHandle finish;
    private final MethodHandle read;
    private final MethodHandle subscribe;
    private final MethodHandle publish;
    final MethodHandle eventNext;
    private final MethodHandle eventInfo;
    private final MethodHandle eventRelease;

    ApplicationFfm(Ffm ffi) {
        this.ffi = ffi;
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment info = scratch.allocate(Ffm.INFO);
            Ffm.check(ffi.query(info));
            if ((info.get(JAVA_LONG, 8) & 14) != 14) throw new NativeException(9);
        }
        state = operation("oj_application_state");
        current = operation("oj_request_current");
        action = ffi.bind("oj_application_action", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_INT));
        generationWatch = ffi.bind("oj_generation_watch", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG, ADDRESS));
        controlNext = operation("oj_control_next");
        controlInfo = operation("oj_control_info");
        controlRelease = handle("oj_control_release");
        join = ffi.bind("oj_application_join", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS, ADDRESS));
        next = operation("oj_application_next");
        closed = operation("oj_application_closed");
        watch = operation("oj_request_watch");
        withdraw = handle("oj_application_withdraw");
        release = handle("oj_application_release");
        requestRelease = handle("oj_request_release");
        requestInfo = operation("oj_request_info");
        requestHeaders = ffi.bind("oj_request_headers", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS, JAVA_LONG));
        respond = ffi.bind("oj_request_respond", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, Ffm.VIEW, ADDRESS));
        read = ffi.bind("oj_request_read", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG, ADDRESS));
        head = ffi.bind("oj_response_head", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, ADDRESS));
        write = ffi.bind("oj_response_write", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, Ffm.VIEW, ADDRESS));
        finish = operation("oj_response_finish");
        subscribe = ffi.bind("oj_event_subscribe", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, Ffm.VIEW, JAVA_INT));
        publish = ffi.bind("oj_event_publish", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, Ffm.VIEW, Ffm.VIEW));
        eventNext = operation("oj_event_next");
        eventInfo = operation("oj_event_info");
        eventRelease = handle("oj_event_release");
        MethodHandle query = ffi.bind("oj_application_abi", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT));
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment info = scratch.allocate(48, 4);
            Ffm.check((int) query.invokeExact(info, 48));
            long[] expected = {48, OPTIONS.byteSize(), OPTIONS.byteAlignment(), INFO.byteSize(), INFO.byteAlignment(), EVENT.byteSize(), EVENT.byteAlignment(), HEADER.byteSize(), CONTROL.byteSize(), CONTROL.byteAlignment(), STATE.byteSize(), STATE.byteAlignment()};
            for (int index = 0; index < expected.length; index++) {
                if (info.get(JAVA_INT, index * 4L) != expected[index]) throw new NativeException(9);
            }
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }

    private MethodHandle handle(String name) { return ffi.bind(name, FunctionDescriptor.of(JAVA_INT, JAVA_LONG)); }
    private MethodHandle operation(String name) { return ffi.bind(name, FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS)); }

    long operation(MethodHandle method, long resource) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment out = scratch.allocate(JAVA_LONG);
            Ffm.check((int) method.invokeExact(resource, out));
            return out.get(JAVA_LONG, 0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }

    long join(long engine, ApplicationOptions options) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment value = scratch.allocate(OPTIONS);
            value.set(JAVA_INT, 0, (int) OPTIONS.byteSize());
            value.set(JAVA_INT, 4, Ffm.ABI);
            value.set(JAVA_LONG, 8, options.incarnation());
            value.set(JAVA_LONG, 16, options.maxExchanges());
            value.set(JAVA_LONG, 24, options.eventCapacity());
            value.set(JAVA_LONG, 32, options.controlCapacity());
            put(value.asSlice(40, 16), scratch, utf8(options.fleet()));
            put(value.asSlice(56, 16), scratch, utf8(options.name()));
            MemorySegment out = scratch.allocate(JAVA_LONG);
            Ffm.check((int) join.invokeExact(engine, value, out));
            return out.get(JAVA_LONG, 0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }

    record Metadata(String method, URI uri, String version, List<Header> headers, long generation) {}

    Metadata request(long resource) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment info = scratch.allocate(INFO);
            Ffm.check((int) requestInfo.invokeExact(resource, info));
            if (info.get(JAVA_INT, 0) != INFO.byteSize()) throw new NativeException(9);
            long count = info.get(JAVA_LONG, 40);
            if (count < 0 || count > Integer.MAX_VALUE) throw new NativeException(9);
            MemorySegment entries = scratch.allocate(Math.multiplyExact(count, HEADER.byteSize()), HEADER.byteAlignment());
            Ffm.check((int) requestHeaders.invokeExact(resource, entries, count));
            List<Header> headers = new ArrayList<>((int) count);
            for (long index = 0; index < count; index++) {
                MemorySegment header = entries.asSlice(index * HEADER.byteSize(), HEADER.byteSize());
                headers.add(Header.owned(decode(copy(header.asSlice(0, 16)), StandardCharsets.US_ASCII), copy(header.asSlice(16, 16))));
            }
            String version = switch (info.get(JAVA_INT, 4)) {
                case 0 -> "0.9"; case 1 -> "1.0"; case 2 -> "1.1"; case 3 -> "2"; case 4 -> "3";
                default -> throw new NativeException(9);
            };
            return new Metadata(decode(copy(info.asSlice(8, 16)), StandardCharsets.UTF_8),
                    URI.create(decode(copy(info.asSlice(24, 16)), StandardCharsets.UTF_8)), version, List.copyOf(headers), info.get(JAVA_LONG, 48));
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }

    long read(long resource, int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("Read capacity must be positive");
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment out = scratch.allocate(JAVA_LONG);
            Ffm.check((int) read.invokeExact(resource, (long) capacity, out));
            return out.get(JAVA_LONG, 0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }

    long write(long resource, byte[] bytes) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment body = scratch.allocate(Ffm.VIEW);
            put(body, scratch, bytes);
            MemorySegment out = scratch.allocate(JAVA_LONG);
            Ffm.check((int) write.invokeExact(resource, body, out));
            return out.get(JAVA_LONG, 0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }

    long respond(long resource, OrbitResponse response) {
        return response(resource, response.status(), response.headers(), response.bytes());
    }

    long head(long resource, int status, List<Header> headers) {
        return response(resource, status, headers, null);
    }

    private long response(long resource, int status, List<Header> headers, byte[] bytes) {
        try (Arena scratch = Arena.ofConfined()) {
            long count = headers.size();
            MemorySegment entries = scratch.allocate(count * HEADER.byteSize(), HEADER.byteAlignment());
            for (int index = 0; index < count; index++) {
                Header header = headers.get(index);
                MemorySegment value = entries.asSlice(index * HEADER.byteSize(), HEADER.byteSize());
                put(value.asSlice(0, 16), scratch, utf8(header.name()));
                put(value.asSlice(16, 16), scratch, header.bytes());
            }
            MemorySegment out = scratch.allocate(JAVA_LONG);
            if (bytes == null) {
                Ffm.check((int) head.invokeExact(resource, status, entries, count, out));
            } else {
                MemorySegment body = scratch.allocate(Ffm.VIEW);
                put(body, scratch, bytes);
                Ffm.check((int) respond.invokeExact(resource, status, entries, count, body, out));
            }
            return out.get(JAVA_LONG, 0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }

    void subscribe(long application, String topic, boolean enabled) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment view = scratch.allocate(Ffm.VIEW);
            put(view, scratch, utf8(topic));
            Ffm.check((int) subscribe.invokeExact(application, view, enabled ? 1 : 0));
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }

    void publish(long application, String topic, String json) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment key = scratch.allocate(Ffm.VIEW);
            MemorySegment payload = scratch.allocate(Ffm.VIEW);
            put(key, scratch, utf8(topic)); put(payload, scratch, utf8(json));
            Ffm.check((int) publish.invokeExact(application, key, payload));
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }

    OrbitEvent event(long resource) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment value = scratch.allocate(EVENT);
            Ffm.check((int) eventInfo.invokeExact(resource, value));
            return new OrbitEvent(decode(copy(value.asSlice(0, 16)), StandardCharsets.UTF_8),
                    decode(copy(value.asSlice(16, 16)), StandardCharsets.UTF_8),
                    decode(copy(value.asSlice(32, 16)), StandardCharsets.UTF_8),
                    () -> Ffm.check(ffi.one(eventRelease, resource)));
        } catch (Throwable failure) {
            Ffm.check(ffi.one(eventRelease, resource));
            throw Ffm.unexpected(failure);
        }
    }

    record State(long generation, boolean running) {}
    State state(long application) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment info = scratch.allocate(STATE);
            Ffm.check((int) state.invokeExact(application, info));
            return new State(info.get(JAVA_LONG, 0), info.get(JAVA_LONG, 8) != 0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    boolean current(long request) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment value = scratch.allocate(JAVA_INT);
            Ffm.check((int) current.invokeExact(request, value));
            return value.get(JAVA_INT, 0) != 0;
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    void action(long application, int value) {
        try { Ffm.check((int) action.invokeExact(application, value)); }
        catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    long watchGeneration(long application, long expected) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment out = scratch.allocate(JAVA_LONG);
            Ffm.check((int) generationWatch.invokeExact(application, expected, out));
            return out.get(JAVA_LONG, 0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    OrbitControl control(long resource) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment value = scratch.allocate(CONTROL);
            Ffm.check((int) controlInfo.invokeExact(resource, value));
            OrbitControl.Kind kind = switch (value.get(JAVA_INT, 0)) {
                case 1 -> OrbitControl.Kind.LIFECYCLE;
                case 2 -> OrbitControl.Kind.COMMAND;
                case 3 -> OrbitControl.Kind.SHUTDOWN;
                default -> throw new NativeException(9);
            };
            OrbitControl.Action actionValue = switch (value.get(JAVA_INT, 4)) {
                case 0 -> null; case 1 -> OrbitControl.Action.START;
                case 2 -> OrbitControl.Action.STOP; case 3 -> OrbitControl.Action.RESTART;
                default -> throw new NativeException(9);
            };
            return new OrbitControl(kind, actionValue, value.get(JAVA_LONG, 8), value.get(JAVA_LONG, 16) != 0,
                    decode(copy(value.asSlice(24, 16)), StandardCharsets.UTF_8), copy(value.asSlice(40, 16)),
                    decode(copy(value.asSlice(56, 16)), StandardCharsets.UTF_8), () -> Ffm.check(ffi.one(controlRelease, resource)));
        } catch (Throwable failure) { Ffm.check(ffi.one(controlRelease, resource)); throw Ffm.unexpected(failure); }
    }

    static byte[] utf8(String value) {
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()]; encoded.get(bytes); return bytes;
        } catch (CharacterCodingException failure) { throw new IllegalArgumentException("Invalid text encoding", failure); }
    }

    static String decode(byte[] value, java.nio.charset.Charset encoding) {
        try { return encoding.newDecoder().onMalformedInput(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(value)).toString(); }
        catch (CharacterCodingException failure) { throw new IllegalArgumentException("Invalid native text encoding", failure); }
    }

    static void put(MemorySegment view, Arena arena, byte[] bytes) {
        MemorySegment value = arena.allocate(bytes.length, 1);
        value.copyFrom(MemorySegment.ofArray(bytes));
        view.set(ADDRESS, 0, value); view.set(JAVA_LONG, 8, bytes.length);
    }

    static byte[] copy(MemorySegment view) {
        long length = view.get(JAVA_LONG, 8);
        MemorySegment pointer = view.get(ADDRESS, 0);
        if (length < 0 || length > Integer.MAX_VALUE || (length != 0 && pointer.address() == 0)) throw new NativeException(9);
        return pointer.reinterpret(length).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
    }
}
