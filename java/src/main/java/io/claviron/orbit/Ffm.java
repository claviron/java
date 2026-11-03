package io.claviron.orbit;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/** Private layouts for the reviewed C header. No application-supplied descriptors. */
@SuppressWarnings("restricted") // Native-access operations are confined to this reviewed boundary.
final class Ffm {
    static final int ABI = 3;
    static final int END = 10;
    static final int ALREADY_COMPLETED = 11;
    static final MemoryLayout INFO = MemoryLayout.structLayout(
            JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_INT, JAVA_INT,
            JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT);
    static final MemoryLayout OPTIONS = MemoryLayout.structLayout(JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG);
    static final MemoryLayout COMPLETION = MemoryLayout.structLayout(JAVA_LONG, JAVA_INT, JAVA_INT, JAVA_LONG, JAVA_LONG);
    static final MemoryLayout VIEW = MemoryLayout.structLayout(ADDRESS, JAVA_LONG);

    final Arena arena = Arena.ofShared();
    final SymbolLookup symbols;
    private final MethodHandle query;
    private final MethodHandle open;
    private final MethodHandle wait;
    private final MethodHandle view;
    final MethodHandle shutdown;
    final MethodHandle releaseEngine;
    final MethodHandle cancel;
    final MethodHandle releaseOperation;
    final MethodHandle releaseBuffer;

    Ffm(Path path) {
        try {
            if (ADDRESS.byteSize() != Long.BYTES) throw new NativeException(9);
            symbols = SymbolLookup.libraryLookup(path.toAbsolutePath(), arena);
            query = bind("oj_abi_query", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT));
            open = bind("oj_engine_open", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
            wait = bind("oj_completion_wait", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS));
            view = bind("oj_buffer_view", FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS));
            shutdown = oneHandle("oj_engine_shutdown");
            releaseEngine = oneHandle("oj_engine_release");
            cancel = oneHandle("oj_operation_cancel");
            releaseOperation = oneHandle("oj_operation_release");
            releaseBuffer = oneHandle("oj_buffer_release");
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment info = scratch.allocate(INFO);
                check(query(info));
                validate(info);
            }
        } catch (Throwable failure) {
            arena.close();
            throw unexpected(failure);
        }
    }

    MethodHandle bind(String name, FunctionDescriptor descriptor) {
        return Linker.nativeLinker().downcallHandle(symbols.find(name).orElseThrow(), descriptor);
    }

    private MethodHandle oneHandle(String name) {
        return bind(name, FunctionDescriptor.of(JAVA_INT, JAVA_LONG));
    }

    int query(MemorySegment info) {
        try {
            return (int) query.invokeExact(info, (int) INFO.byteSize());
        } catch (Throwable failure) {
            throw unexpected(failure);
        }
    }

    static void validate(MemorySegment info) {
        if (info.get(JAVA_INT, 0) != INFO.byteSize() || info.get(JAVA_INT, 4) != ABI
                || (info.get(JAVA_LONG, 8) & 1) == 0
                || info.get(JAVA_INT, 16) != OPTIONS.byteSize() || info.get(JAVA_INT, 20) != OPTIONS.byteAlignment()
                || info.get(JAVA_INT, 24) != COMPLETION.byteSize() || info.get(JAVA_INT, 28) != COMPLETION.byteAlignment()
                || info.get(JAVA_INT, 32) != VIEW.byteSize() || info.get(JAVA_INT, 36) != VIEW.byteAlignment()) {
            throw new NativeException(9);
        }
    }

    long open(int operations, long bytes) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment options = scratch.allocate(OPTIONS);
            options.set(JAVA_INT, 0, (int) OPTIONS.byteSize());
            options.set(JAVA_INT, 4, ABI);
            options.set(JAVA_LONG, 8, operations);
            options.set(JAVA_LONG, 16, bytes);
            MemorySegment out = scratch.allocate(JAVA_LONG);
            check((int) open.invokeExact(options, out));
            return out.get(JAVA_LONG, 0);
        } catch (Throwable failure) {
            throw unexpected(failure);
        }
    }

    int wait(long engine, MemorySegment out) {
        try {
            return (int) wait.invokeExact(engine, out);
        } catch (Throwable failure) {
            throw unexpected(failure);
        }
    }

    MemorySegment view(long handle, Arena lifetime) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment out = scratch.allocate(VIEW);
            check((int) view.invokeExact(handle, out));
            long length = out.get(JAVA_LONG, 8);
            MemorySegment address = out.get(ADDRESS, 0);
            if (length < 0 || (length != 0 && address.address() == 0)) throw new NativeException(9);
            return address.reinterpret(length, lifetime, null).asReadOnly();
        } catch (Throwable failure) {
            throw unexpected(failure);
        }
    }

    int one(MethodHandle function, long handle) {
        try {
            return (int) function.invokeExact(handle);
        } catch (Throwable failure) {
            throw unexpected(failure);
        }
    }

    static void check(int status) {
        if (status != 0) throw new NativeException(status);
    }

    static RuntimeException unexpected(Throwable failure) {
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException exception) return exception;
        return new IllegalStateException("FFM invocation failed", failure);
    }
}
