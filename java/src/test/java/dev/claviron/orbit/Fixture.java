package dev.claviron.orbit;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/** Test-only controlled producer. Symbols do not exist in the production addon. */
final class Fixture {
    private final MethodHandle submit;
    private final MethodHandle complete;

    Fixture(NativeLibrary library) {
        submit = library.ffi.bind("oj_fixture_submit",
                FunctionDescriptor.of(JAVA_INT, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS));
        complete = library.ffi.bind("oj_fixture_complete", FunctionDescriptor.of(JAVA_INT, JAVA_LONG));
    }

    NativeOperation submit(NativeRuntime runtime, byte[] bytes) {
        return submit(runtime, bytes, false);
    }

    NativeOperation submitReady(NativeRuntime runtime, byte[] bytes) {
        return submit(runtime, bytes, true);
    }

    private NativeOperation submit(NativeRuntime runtime, byte[] bytes, boolean ready) {
        return runtime.submit(engine -> {
            try (Arena scratch = Arena.ofConfined()) {
                MemorySegment input = scratch.allocate(bytes.length, 1);
                input.copyFrom(MemorySegment.ofArray(bytes));
                MemorySegment out = scratch.allocate(JAVA_LONG);
                Ffm.check((int) submit.invokeExact(engine, input, (long) bytes.length, out));
                long operation = out.get(JAVA_LONG, 0);
                if (ready) Ffm.check((int) complete.invokeExact(operation));
                return operation;
            } catch (Throwable failure) {
                throw Ffm.unexpected(failure);
            }
        });
    }

    void complete(NativeOperation operation) {
        try {
            Ffm.check((int) complete.invokeExact(operation.handle));
        } catch (Throwable failure) {
            throw Ffm.unexpected(failure);
        }
    }
}
