package dev.claviron.orbit;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import static java.lang.foreign.ValueLayout.*;

/** Reviewed invoke ABI, separate from the HTTP descriptor set. */
@SuppressWarnings("restricted")
final class InvokeFfm {
    private static final MemoryLayout OPTIONS = MemoryLayout.structLayout(JAVA_INT,JAVA_INT,JAVA_LONG,JAVA_LONG,JAVA_LONG,JAVA_LONG,JAVA_LONG,Ffm.VIEW,Ffm.VIEW);
    private static final MemoryLayout ROUTE = MemoryLayout.structLayout(Ffm.VIEW,Ffm.VIEW);
    private static final MemoryLayout INFO = MemoryLayout.structLayout(Ffm.VIEW,Ffm.VIEW,JAVA_LONG);
    private static final MemoryLayout RESULT = MemoryLayout.structLayout(JAVA_INT,JAVA_INT,Ffm.VIEW);
    private final Ffm ffi;
    private final MethodHandle open, call, info, current, reply, resultInfo, resultRelease;
    final MethodHandle next, withdraw, closed, release, requestRelease, watch;
    InvokeFfm(Ffm ffi) {
        this.ffi = ffi;
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment meta = scratch.allocate(Ffm.INFO); Ffm.check(ffi.query(meta));
            if ((meta.get(JAVA_LONG,8) & 16) == 0) throw new NativeException(9);
        }
        open = ffi.bind("oj_invoke_open", FunctionDescriptor.of(JAVA_INT,JAVA_LONG,ADDRESS,ADDRESS,JAVA_LONG,ADDRESS));
        call = ffi.bind("oj_invoke_call", FunctionDescriptor.of(JAVA_INT,JAVA_LONG,Ffm.VIEW,Ffm.VIEW,Ffm.VIEW,ADDRESS));
        next = operation("oj_invoke_next"); closed = operation("oj_invoke_closed");
        withdraw = handle("oj_invoke_withdraw"); release = handle("oj_invoke_release");
        requestRelease = handle("oj_invocation_release"); watch = operation("oj_invocation_watch");
        info = operation("oj_invocation_info"); current = operation("oj_invocation_current");
        reply = ffi.bind("oj_invocation_reply", FunctionDescriptor.of(JAVA_INT,JAVA_LONG,JAVA_INT,Ffm.VIEW));
        resultInfo = operation("oj_invoke_result_info"); resultRelease = handle("oj_invoke_result_release");
        MethodHandle query = ffi.bind("oj_invoke_abi", FunctionDescriptor.of(JAVA_INT,ADDRESS,JAVA_INT));
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment value = scratch.allocate(32,4);
            Ffm.check((int) query.invokeExact(value,32));
            long[] expected = {32,OPTIONS.byteSize(),OPTIONS.byteAlignment(),INFO.byteSize(),INFO.byteAlignment(),RESULT.byteSize(),RESULT.byteAlignment(),ROUTE.byteSize()};
            for (int i=0;i<expected.length;i++) if (value.get(JAVA_INT,i*4L)!=expected[i]) throw new NativeException(9);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    private MethodHandle handle(String name) { return ffi.bind(name,FunctionDescriptor.of(JAVA_INT,JAVA_LONG)); }
    private MethodHandle operation(String name) { return ffi.bind(name,FunctionDescriptor.of(JAVA_INT,JAVA_LONG,ADDRESS)); }
    long operation(MethodHandle method,long resource) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment out=scratch.allocate(JAVA_LONG); Ffm.check((int)method.invokeExact(resource,out)); return out.get(JAVA_LONG,0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    private static MemorySegment view(Arena scratch,byte[] bytes) {
        MemorySegment value=scratch.allocate(Ffm.VIEW); ApplicationFfm.put(value,scratch,bytes); return value;
    }
    long open(long application,InvokeOptions options) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment value=scratch.allocate(OPTIONS); value.set(JAVA_INT,0,(int)OPTIONS.byteSize()); value.set(JAVA_INT,4,Ffm.ABI);
            long[] limits={options.pendingCalls(),options.acceptedCalls(),options.operationBytes(),options.requestBytes(),options.responseBytes()};
            for (int i=0;i<limits.length;i++) value.set(JAVA_LONG,8+i*8L,limits[i]);
            ApplicationFfm.put(value.asSlice(48,16),scratch,ApplicationFfm.utf8(options.fleet()));
            ApplicationFfm.put(value.asSlice(64,16),scratch,ApplicationFfm.utf8(options.service()));
            MemorySegment routes=scratch.allocate(options.routes().size()*ROUTE.byteSize(),ROUTE.byteAlignment());
            int index=0;
            for (var route:options.routes().entrySet()) {
                MemorySegment entry=routes.asSlice(index++*ROUTE.byteSize(),ROUTE.byteSize());
                ApplicationFfm.put(entry.asSlice(0,16),scratch,ApplicationFfm.utf8(route.getKey()));
                ApplicationFfm.put(entry.asSlice(16,16),scratch,ApplicationFfm.utf8(route.getValue()));
            }
            MemorySegment out=scratch.allocate(JAVA_LONG);
            Ffm.check((int)open.invokeExact(application,value,routes,(long)options.routes().size(),out)); return out.get(JAVA_LONG,0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    long call(long binding,String target,String operation,byte[] payload) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment out=scratch.allocate(JAVA_LONG);
            Ffm.check((int)call.invokeExact(binding,view(scratch,ApplicationFfm.utf8(target)),view(scratch,ApplicationFfm.utf8(operation)),view(scratch,payload),out));
            return out.get(JAVA_LONG,0);
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    record Metadata(String operation,byte[] payload,long generation) {}
    Metadata info(long resource) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment value=scratch.allocate(INFO); Ffm.check((int)info.invokeExact(resource,value));
            return new Metadata(ApplicationFfm.decode(ApplicationFfm.copy(value.asSlice(0,16)),StandardCharsets.UTF_8),
                    ApplicationFfm.copy(value.asSlice(16,16)),value.get(JAVA_LONG,32));
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    boolean current(long resource) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment out=scratch.allocate(JAVA_INT); Ffm.check((int)current.invokeExact(resource,out)); return out.get(JAVA_INT,0)!=0;
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    void reply(long resource,InvokeReply response) {
        try (Arena scratch = Arena.ofConfined()) { Ffm.check((int)reply.invokeExact(resource,response.code(),view(scratch,response.bytes()))); }
        catch (Throwable failure) { throw Ffm.unexpected(failure); }
    }
    InvokeResult result(long resource) {
        try (Arena scratch = Arena.ofConfined()) {
            MemorySegment value=scratch.allocate(RESULT); Ffm.check((int)resultInfo.invokeExact(resource,value));
            int code=value.get(JAVA_INT,0);
            if (code<0 || code>=InvokeResult.Code.values().length || value.get(JAVA_INT,4)!=0) throw new NativeException(9);
            return new InvokeResult(InvokeResult.Code.values()[code],ApplicationFfm.copy(value.asSlice(8,16)));
        } catch (Throwable failure) { throw Ffm.unexpected(failure); }
        finally { Ffm.check(ffi.one(resultRelease,resource)); }
    }
}
